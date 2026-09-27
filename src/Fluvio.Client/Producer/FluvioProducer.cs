using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using Fluvio.Client.Abstractions;
using Fluvio.Client.Interop;
using Fluvio.Client.Telemetry;

namespace Fluvio.Client.Producer;

/// <summary>
/// Fluvio producer implementation. Sends and flushes go through the native Rust FFI layer
/// (see <see cref="Interop"/>), which wraps the official <c>fluvio</c> Rust client's
/// <c>TopicProducerPool</c>.
/// </summary>
internal sealed class FluvioProducer : IFluvioProducer
{
    private readonly RustResource _clientHandle;
    private readonly ProducerOptions _options;
    private readonly object _handlesLock = new();
    private readonly Dictionary<string, Task<RustResource>> _producerHandleTasksByTopic = new();
    private readonly ConcurrentDictionary<string, int> _partitionCounts = new();
    private bool _disposed;

    /// <summary>
    /// Timeout applied to every send/flush call via <see cref="CreateSendTimeoutSource"/>.
    /// </summary>
    internal static readonly TimeSpan SendTimeout = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Links <paramref name="callerToken"/> with <paramref name="timeout"/> (30s by default via
    /// <see cref="SendTimeout"/>) so a stalled native send/flush call cannot hang forever while
    /// caller cancellation still propagates. <paramref name="timeout"/> is a parameter (rather than
    /// always reading <see cref="SendTimeout"/> directly) purely so tests can exercise a short
    /// timeout without mutating shared static state, which previously leaked across concurrently
    /// executing test classes in the same process.
    /// </summary>
    internal static CancellationTokenSource CreateSendTimeoutSource(CancellationToken callerToken, TimeSpan? timeout = null)
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(callerToken);
        cts.CancelAfter(timeout ?? SendTimeout);
        return cts;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="FluvioProducer"/> class.
    /// </summary>
    /// <param name="clientHandle">The native client handle producers are created against.</param>
    /// <param name="options">Producer options.</param>
    public FluvioProducer(RustResource clientHandle, ProducerOptions? options)
    {
        _clientHandle = clientHandle;
        _options = options ?? new ProducerOptions();
    }

    /// <summary>
    /// Sends a record to the specified topic via the native producer.
    /// </summary>
    /// <param name="topic">Topic name.</param>
    /// <param name="value">Record value.</param>
    /// <param name="key">Optional record key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Offset of the produced record.</returns>
    public async Task<long> SendAsync(string topic, ReadOnlyMemory<byte> value, ReadOnlyMemory<byte>? key = null, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        using var activity = FluvioActivitySource.Instance.StartActivity(
            FluvioActivitySource.Operations.Produce,
            ActivityKind.Producer);

        activity?.SetTag(FluvioActivitySource.Tags.MessagingSystem, "fluvio");
        activity?.SetTag(FluvioActivitySource.Tags.MessagingOperation, "publish");
        activity?.SetTag(FluvioActivitySource.Tags.MessagingDestination, topic);
        activity?.SetTag(FluvioActivitySource.Tags.Topic, topic);
        activity?.SetTag(FluvioActivitySource.Tags.RecordCount, 1);

        try
        {
            using var timeoutCts = CreateSendTimeoutSource(cancellationToken);
            var producerHandle = await GetOrCreateProducerHandleAsync(topic, timeoutCts.Token).ConfigureAwait(false);
            var valueArray = value.ToArray();
            var keyArray = key?.ToArray();

            long explicitPartition = -1;
            if (_options.Partitioner is { } partitioner)
            {
                var partitionCount = _partitionCounts.GetValueOrDefault(topic, 1);
                var config = new PartitionerConfig(partitionCount);
                explicitPartition = partitioner.SelectPartition(topic, key, value, config);
            }

            var (cancelHandle, registration) = CancellationBridge.Create(timeoutCts.Token);
            using var _cancelReg = registration;
            var offset = await producerHandle.RunAsyncWithIncrement(async h =>
            {
                Task<nint> callTask;
                unsafe
                {
                    fixed (byte* vp = valueArray)
                    fixed (byte* kp = keyArray)
                    {
                        var valueAddr = (nint)vp;
                        var keyAddr = (nint)kp;
                        callTask = Callbacks.CallAsync(tcb =>
                        {
                            unsafe
                            {
                                Native.ProducerSend(
                                    h,
                                    (byte*)keyAddr, (nuint)(keyArray?.Length ?? 0),
                                    (byte*)valueAddr, (nuint)valueArray.Length,
                                    explicitPartition,
                                    cancelHandle,
                                    tcb);
                            }
                        });
                    }
                }
                var resultPtr = await callTask.ConfigureAwait(false);
                return (long)resultPtr;
            }).ConfigureAwait(false);

            activity?.SetTag(FluvioActivitySource.Tags.Offset, offset);
            activity?.SetStatus(ActivityStatusCode.Ok);

            return offset;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw;
        }
    }

    /// <summary>
    /// Sends a batch of records to the specified topic by sending each record individually.
    /// </summary>
    /// <param name="topic">Topic name.</param>
    /// <param name="records">Records to produce.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>List of offsets for produced records.</returns>
    public async Task<IReadOnlyList<long>> SendBatchAsync(string topic, IEnumerable<ProduceRecord> records, CancellationToken cancellationToken = default)
    {
        var offsets = new List<long>();
        foreach (var record in records)
        {
            var offset = await SendAsync(topic, record.Value, record.Key, cancellationToken).ConfigureAwait(false);
            offsets.Add(offset);
        }

        return offsets;
    }

    /// <summary>
    /// Flushes all producers created by this instance.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        List<Task<RustResource>> handleTasks;
        lock (_handlesLock)
        {
            handleTasks = [.. _producerHandleTasksByTopic.Values];
        }

        foreach (var handleTask in handleTasks)
        {
            var handle = await handleTask.ConfigureAwait(false);
            using var timeoutCts = CreateSendTimeoutSource(cancellationToken);
            var (cancelHandle, registration) = CancellationBridge.Create(timeoutCts.Token);
            using var _ = registration;
            await handle.RunAsyncWithIncrement(h =>
                Callbacks.CallAsync(tcb => Native.ProducerFlush(h, cancelHandle, tcb))).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Sets the partition count for a topic, enabling <see cref="ProducerOptions.Partitioner"/> to
    /// compute a <see cref="PartitionerConfig"/> with the real partition count for this topic.
    /// </summary>
    /// <remarks>
    /// Only meaningful when <see cref="ProducerOptions.Partitioner"/> is set — without a custom
    /// partitioner, the native <c>fluvio</c> client's own default partitioner is used unchanged and
    /// this value is not consulted. Must be called before <see cref="SendAsync"/> for callers that
    /// want their <see cref="IPartitioner"/> to see the topic's actual partition count rather than
    /// the default of 1.
    /// </remarks>
    /// <param name="topic">Topic name.</param>
    /// <param name="partitionCount">Number of partitions.</param>
    public void SetPartitionCount(string topic, int partitionCount)
    {
        if (partitionCount <= 0)
        {
            throw new ArgumentException("Partition count must be positive", nameof(partitionCount));
        }

        _partitionCounts[topic] = partitionCount;
    }

    private Task<RustResource> GetOrCreateProducerHandleAsync(string topic, CancellationToken cancellationToken)
    {
        lock (_handlesLock)
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(FluvioProducer));
            }

            if (_producerHandleTasksByTopic.TryGetValue(topic, out var existingTask))
            {
                return existingTask;
            }

            var creationTask = CreateProducerHandleAsync(topic, cancellationToken);
            _producerHandleTasksByTopic[topic] = creationTask;

            // A failed/cancelled creation must not permanently poison this topic — remove it so the
            // next SendAsync call retries fresh, matching the retry-on-failure behavior of the
            // original semaphore-guarded implementation (which only cached on success).
            _ = creationTask.ContinueWith(t =>
            {
                if (t.IsCompletedSuccessfully)
                {
                    return;
                }
                lock (_handlesLock)
                {
                    if (_producerHandleTasksByTopic.TryGetValue(topic, out var current) && current == creationTask)
                    {
                        _producerHandleTasksByTopic.Remove(topic);
                    }
                }
            }, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);

            return creationTask;
        }
    }

    private async Task<RustResource> CreateProducerHandleAsync(string topic, CancellationToken cancellationToken)
    {
        var topicBytes = Encoding.UTF8.GetBytes(topic);
        var useExplicitPartitioning = (byte)(_options.Partitioner is not null ? 1 : 0);
        var (cancelHandle, registration) = CancellationBridge.Create(cancellationToken);
        using var _cancelReg = registration;
        var resultPtr = await _clientHandle.RunAsyncWithIncrement(async ch =>
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
                            Native.ProducerNew(ch, (byte*)topicAddr, (nuint)topicBytes.Length, useExplicitPartitioning, cancelHandle, tcb);
                        }
                    });
                }
            }
            return await callTask.ConfigureAwait(false);
        }).ConfigureAwait(false);

        return new RustResource(resultPtr, Native.ProducerDrop);
    }

    /// <summary>
    /// Disposes the producer, flushing and releasing every native producer handle it created.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        List<Task<RustResource>> handleTasks;
        lock (_handlesLock)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            handleTasks = [.. _producerHandleTasksByTopic.Values];
        }

        // Awaiting every tracked creation task here (not just already-completed handles) is what
        // closes the race: a SendAsync that acquired _handlesLock and registered its creation task
        // BEFORE this method flipped _disposed is guaranteed to be in handleTasks, so its handle is
        // always flushed and disposed rather than leaked. A creation that fails/is cancelled after
        // this snapshot just throws here, which is fine - there is no handle to clean up for it.
        foreach (var handleTask in handleTasks)
        {
            RustResource handle;
            try
            {
                handle = await handleTask.ConfigureAwait(false);
            }
            catch
            {
                continue;
            }

            try
            {
                var (cancelHandle, registration) = CancellationBridge.Create(CancellationToken.None);
                using var _ = registration;
                await handle.RunAsyncWithIncrement(h =>
                    Callbacks.CallAsync(tcb => Native.ProducerFlush(h, cancelHandle, tcb))).ConfigureAwait(false);
            }
            catch
            {
                // Ignore errors during disposal flush.
            }

            handle.Dispose();
        }
    }
}
