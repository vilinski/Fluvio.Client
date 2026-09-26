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
    private readonly ConcurrentDictionary<string, RustResource> _producerHandlesByTopic = new();
    private readonly SemaphoreSlim _producerCreationLock = new(1, 1);
    private bool _disposed;

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
            var producerHandle = await GetOrCreateProducerHandleAsync(topic, cancellationToken).ConfigureAwait(false);
            var valueArray = value.ToArray();
            var keyArray = key?.ToArray();

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

        foreach (var handle in _producerHandlesByTopic.Values)
        {
            await handle.RunAsyncWithIncrement(h =>
                Callbacks.CallAsync(tcb => Native.ProducerFlush(h, tcb))).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Sets the partition count for a topic, enabling the partitioner to work correctly.
    /// </summary>
    /// <remarks>
    /// Partition selection is now performed by the native <c>fluvio</c> client based on the
    /// record key, so this no longer affects producer behavior; it is retained to satisfy
    /// <see cref="IFluvioProducer"/> for callers that still call it.
    /// </remarks>
    /// <param name="topic">Topic name.</param>
    /// <param name="partitionCount">Number of partitions.</param>
    public void SetPartitionCount(string topic, int partitionCount)
    {
        if (partitionCount <= 0)
        {
            throw new ArgumentException("Partition count must be positive", nameof(partitionCount));
        }
    }

    private async Task<RustResource> GetOrCreateProducerHandleAsync(string topic, CancellationToken cancellationToken)
    {
        if (_producerHandlesByTopic.TryGetValue(topic, out var existing))
        {
            return existing;
        }

        await _producerCreationLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_producerHandlesByTopic.TryGetValue(topic, out existing))
            {
                return existing;
            }

            var topicBytes = Encoding.UTF8.GetBytes(topic);
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
                                Native.ProducerNew(ch, (byte*)topicAddr, (nuint)topicBytes.Length, tcb);
                            }
                        });
                    }
                }
                return await callTask.ConfigureAwait(false);
            }).ConfigureAwait(false);

            var handle = new RustResource(resultPtr, Native.ProducerDrop);
            _producerHandlesByTopic[topic] = handle;
            return handle;
        }
        finally
        {
            _producerCreationLock.Release();
        }
    }

    /// <summary>
    /// Disposes the producer, flushing and releasing every native producer handle it created.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        foreach (var handle in _producerHandlesByTopic.Values)
        {
            try
            {
                await handle.RunAsyncWithIncrement(h =>
                    Callbacks.CallAsync(tcb => Native.ProducerFlush(h, tcb))).ConfigureAwait(false);
            }
            catch
            {
                // Ignore errors during disposal flush.
            }

            handle.Dispose();
        }

        _producerCreationLock.Dispose();
    }
}
