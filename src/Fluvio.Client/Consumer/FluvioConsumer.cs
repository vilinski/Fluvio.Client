using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using Fluvio.Client.Abstractions;
using Fluvio.Client.Interop;
using Fluvio.Client.Telemetry;
using Microsoft.Extensions.Logging;

namespace Fluvio.Client.Consumer;

/// <summary>
/// Fluvio consumer implementation. All operations go through the native Rust FFI layer
/// (see <see cref="Interop"/>), which wraps the official <c>fluvio</c> Rust client.
/// </summary>
internal sealed class FluvioConsumer : IFluvioConsumer
{
    private readonly RustResource _clientHandle;
    private readonly ConsumerOptions _options;
    private readonly string? _clientId;
    private readonly ILogger? _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="FluvioConsumer"/> class.
    /// </summary>
    /// <param name="clientHandle">The native client handle fetch/offset operations are performed against.</param>
    /// <param name="options">Consumer options.</param>
    /// <param name="clientId">Optional client ID.</param>
    /// <param name="logger">Optional logger.</param>
    public FluvioConsumer(RustResource clientHandle, ConsumerOptions? options, string? clientId, ILogger? logger = null)
    {
        _clientHandle = clientHandle;
        _options = options ?? new ConsumerOptions();
        _clientId = clientId;
        _logger = logger;
    }

    /// <summary>
    /// Streams records from the specified topic starting at the given offset.
    /// </summary>
    /// <param name="topic">Topic name.</param>
    /// <param name="partition">Partition number.</param>
    /// <param name="offset">Starting offset (null to use offset reset strategy).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Async enumerable of consumed records.</returns>
    public async IAsyncEnumerable<ConsumeRecord> StreamAsync(
        string topic,
        int partition = 0,
        long? offset = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        long? storedOffset = null;
        // Deliberately NOT OffsetResolver.GetConsumerId(_options.ConsumerGroup): that helper appends a
        // random per-call instance suffix, which would make every StreamAsync call resolve to a
        // different consumer identity and never find a previously committed offset. Offset persistence
        // needs a deterministic identity shared across resumed sessions, so the consumer group name
        // itself is used directly (matching how a Kafka-style consumer group shares one committed
        // offset per topic/partition across its members, rather than per instance).
        var consumerId = string.IsNullOrEmpty(_options.ConsumerGroup) ? null : _options.ConsumerGroup;
        if (offset is null && consumerId is not null &&
            _options.OffsetReset is OffsetResetStrategy.StoredOrEarliest or OffsetResetStrategy.StoredOrLatest)
        {
            try
            {
                storedOffset = await FetchLastOffsetAsync(consumerId, topic, partition, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Failed to fetch stored offset for {ConsumerId}/{Topic}:{Partition}, falling back to reset strategy", consumerId, topic, partition);
            }
        }
        var startOffset = offset ?? OffsetResolver.ResolveStartOffset(storedOffset, _options.OffsetReset);
        var topicBytes = Encoding.UTF8.GetBytes(topic);

        var streamPtr = await _clientHandle.RunAsyncWithIncrement(async h =>
        {
            Task<nint> callTask;
            unsafe
            {
                fixed (byte* tp = topicBytes)
                {
                    var topicAddr = (nint)tp;
                    callTask = Callbacks.CallAsync(tcb =>
                    {
                        unsafe
                        {
                            Native.StreamNew(h, (byte*)topicAddr, (nuint)topicBytes.Length, (uint)partition, startOffset, tcb);
                        }
                    });
                }
            }
            return await callTask.ConfigureAwait(false);
        }).ConfigureAwait(false);

        var streamHandle = new RustResource(streamPtr, Native.StreamDrop);
        await using var registration = cancellationToken.CanBeCanceled
            ? cancellationToken.Register(
                static state => ((RustResource)state!).RunWithIncrement(h =>
                {
                    Native.StreamClose(h);
                    return 0;
                }),
                streamHandle)
            : default;

        // Deliberate deviation from the plan's sketch: cancellation is allowed to propagate as
        // an `OperationCanceledException` out of `MoveNextAsync` rather than being swallowed into
        // a silent `yield break`. Swallowing it would make `await foreach` complete normally on
        // cancellation, which is both non-idiomatic for a cancellable async-iterator and would
        // break existing callers (e.g. the empty-topic streaming test) that rely on catching the
        // exception to detect a timeout/cancellation. `Native.StreamClose` still runs first via
        // the `cancellationToken.Register` callback above (before `streamHandle.Dispose()` in the
        // `finally` below), and the native `tokio::select!` still completes the TCB exactly once,
        // satisfying the plan's cancellation-race requirement regardless of how the resulting
        // exception is handled on the C# side.
        try
        {
            while (true)
            {
                var recordPtr = await streamHandle.RunAsyncWithIncrement(h =>
                    Callbacks.CallAsync(tcb => Native.StreamNext(h, tcb))).ConfigureAwait(false);

                if (recordPtr == 0) yield break;

                yield return NativeBuffer.ToConsumeRecord(recordPtr, partition);
            }
        }
        finally
        {
            streamHandle.Dispose();
        }
    }

    /// <summary>
    /// Fetches a batch of records from the specified topic via the native consumer.
    /// </summary>
    /// <param name="topic">Topic name.</param>
    /// <param name="partition">Partition number.</param>
    /// <param name="offset">Starting offset.</param>
    /// <param name="maxBytes">Maximum bytes to fetch.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>List of consumed records.</returns>
    public async Task<IReadOnlyList<ConsumeRecord>> FetchBatchAsync(
        string topic,
        int partition = 0,
        long offset = 0,
        int maxBytes = 1024 * 1024,
        CancellationToken cancellationToken = default)
    {
        using var activity = FluvioActivitySource.Instance.StartActivity(
            FluvioActivitySource.Operations.Consume,
            ActivityKind.Consumer);

        activity?.SetTag(FluvioActivitySource.Tags.MessagingSystem, "fluvio");
        activity?.SetTag(FluvioActivitySource.Tags.MessagingOperation, "receive");
        activity?.SetTag(FluvioActivitySource.Tags.MessagingDestination, topic);
        activity?.SetTag(FluvioActivitySource.Tags.Topic, topic);
        activity?.SetTag(FluvioActivitySource.Tags.Partition, partition);
        activity?.SetTag(FluvioActivitySource.Tags.Offset, offset);

        var (cancelHandle, registration) = CancellationBridge.Create(cancellationToken);
        try
        {
            using var _ = registration;
            var topicBytes = Encoding.UTF8.GetBytes(topic);
            var arrayPtr = await _clientHandle.RunAsyncWithIncrement(async h =>
            {
                Task<nint> callTask;
                unsafe
                {
                    fixed (byte* tp = topicBytes)
                    {
                        var topicAddr = (nint)tp;
                        callTask = Callbacks.CallAsync(tcb =>
                        {
                            unsafe
                            {
                                Native.ConsumerFetchBatch(h, (byte*)topicAddr, (nuint)topicBytes.Length, (uint)partition, offset, (uint)maxBytes, cancelHandle, tcb);
                            }
                        });
                    }
                }
                return await callTask.ConfigureAwait(false);
            }).ConfigureAwait(false);

            var records = NativeBuffer.ReadRecordArrayAndFree(arrayPtr, partition);

            activity?.SetTag(FluvioActivitySource.Tags.RecordCount, records.Count);
            activity?.SetStatus(ActivityStatusCode.Ok);

            return records;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw;
        }
    }

    /// <summary>
    /// Fetches the last committed offset for a consumer from the cluster.
    /// </summary>
    public async Task<long?> FetchLastOffsetAsync(
        string consumerId,
        string topic,
        int partition = 0,
        CancellationToken cancellationToken = default)
    {
        var idBytes = Encoding.UTF8.GetBytes(consumerId);
        var topicBytes = Encoding.UTF8.GetBytes(topic);
        var (cancelHandle, registration) = CancellationBridge.Create(cancellationToken);
        using var _ = registration;
        var resultPtr = await _clientHandle.RunAsyncWithIncrement(async h =>
        {
            Task<nint> callTask;
            unsafe
            {
                fixed (byte* ip = idBytes)
                fixed (byte* tp = topicBytes)
                {
                    var idAddr = (nint)ip;
                    var topicAddr = (nint)tp;
                    callTask = Callbacks.CallAsync(tcb =>
                    {
                        unsafe
                        {
                            Native.ConsumerFetchLastOffset(h, (byte*)idAddr, (nuint)idBytes.Length, (byte*)topicAddr, (nuint)topicBytes.Length, (uint)partition, cancelHandle, tcb);
                        }
                    });
                }
            }
            return await callTask.ConfigureAwait(false);
        }).ConfigureAwait(false);

        var value = (long)resultPtr;
        return value < 0 ? null : value;
    }

    /// <summary>
    /// Commits (updates) the consumer offset for a specific topic/partition.
    /// </summary>
    public async Task CommitOffsetAsync(
        string consumerId,
        string topic,
        int partition,
        long offset,
        CancellationToken cancellationToken = default)
    {
        var idBytes = Encoding.UTF8.GetBytes(consumerId);
        var topicBytes = Encoding.UTF8.GetBytes(topic);
        var (cancelHandle, registration) = CancellationBridge.Create(cancellationToken);
        using var _ = registration;
        await _clientHandle.RunAsyncWithIncrement(async h =>
        {
            Task<nint> callTask;
            unsafe
            {
                fixed (byte* ip = idBytes)
                fixed (byte* tp = topicBytes)
                {
                    var idAddr = (nint)ip;
                    var topicAddr = (nint)tp;
                    callTask = Callbacks.CallAsync(tcb =>
                    {
                        unsafe
                        {
                            Native.ConsumerCommitOffset(h, (byte*)idAddr, (nuint)idBytes.Length, (byte*)topicAddr, (nuint)topicBytes.Length, (uint)partition, offset, cancelHandle, tcb);
                        }
                    });
                }
            }
            return await callTask.ConfigureAwait(false);
        }).ConfigureAwait(false);
    }

    /// <summary>
    /// Disposes the consumer. (No-op, does not own the client handle.)
    /// </summary>
    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }
}
