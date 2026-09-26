using System.Diagnostics;
using System.Text;
using Fluvio.Client.Abstractions;
using Fluvio.Client.Interop;
using Fluvio.Client.Telemetry;
using Microsoft.Extensions.Logging;

namespace Fluvio.Client.Consumer;

/// <summary>
/// Fluvio consumer implementation. Fetch/offset operations go through the native Rust FFI layer
/// (see <see cref="Interop"/>), which wraps the official <c>fluvio</c> Rust client.
/// </summary>
/// <remarks>
/// <see cref="StreamAsync"/> is not yet backed by the native FFI layer; it will be wired up
/// (and <c>StreamingConsumer</c> deleted) when the consumer streaming FFI is implemented.
/// </remarks>
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
    public IAsyncEnumerable<ConsumeRecord> StreamAsync(
        string topic,
        int partition = 0,
        long? offset = null,
        CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException(
            "StreamAsync is not yet backed by the native FFI layer; it will be wired up when the consumer streaming FFI is implemented.");
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

        try
        {
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
                                Native.ConsumerFetchBatch(h, (byte*)topicAddr, (nuint)topicBytes.Length, (uint)partition, offset, (uint)maxBytes, tcb);
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
                            Native.ConsumerFetchLastOffset(h, (byte*)idAddr, (nuint)idBytes.Length, (byte*)topicAddr, (nuint)topicBytes.Length, (uint)partition, tcb);
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
                            Native.ConsumerCommitOffset(h, (byte*)idAddr, (nuint)idBytes.Length, (byte*)topicAddr, (nuint)topicBytes.Length, (uint)partition, offset, tcb);
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
