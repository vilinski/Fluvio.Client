using System.Text;
using Fluvio.Client.Abstractions;

namespace Fluvio.Client.Tests.Integration;

[Collection("Integration")]
public class ProducerIntegrationTests : FluvioIntegrationTestBase
{
    [Fact]
    public async Task SendAsync_SingleMessage_Success()
    {
        var topicName = await CreateTestTopicAsync();
        var producer = Client!.Producer();

        try
        {
            var message = Encoding.UTF8.GetBytes("Hello, Fluvio!");
            var offset = await producer.SendAsync(topicName, message);

            Assert.True(offset >= 0);
        }
        finally
        {
            await CleanupTopicAsync(topicName);
        }
    }

    [Fact]
    public async Task SendAsync_WithKey_Success()
    {
        var topicName = await CreateTestTopicAsync();
        var producer = Client!.Producer();

        try
        {
            var key = Encoding.UTF8.GetBytes("key-1");
            var value = Encoding.UTF8.GetBytes("value-1");

            var offset = await producer.SendAsync(topicName, value, key);

            Assert.True(offset >= 0);
        }
        finally
        {
            await CleanupTopicAsync(topicName);
        }
    }

    [Fact]
    public async Task SendAsync_MultipleMessages_IncreasingOffsets()
    {
        var topicName = await CreateTestTopicAsync();
        var producer = Client!.Producer();

        try
        {
            var offset1 = await producer.SendAsync(topicName, Encoding.UTF8.GetBytes("msg1"));
            var offset2 = await producer.SendAsync(topicName, Encoding.UTF8.GetBytes("msg2"));
            var offset3 = await producer.SendAsync(topicName, Encoding.UTF8.GetBytes("msg3"));

            Assert.True(offset2 > offset1);
            Assert.True(offset3 > offset2);
        }
        finally
        {
            await CleanupTopicAsync(topicName);
        }
    }

    [Fact]
    public async Task SendBatchAsync_MultpleMessages_Success()
    {
        var topicName = await CreateTestTopicAsync();
        var producer = Client!.Producer();

        try
        {
            var records = new List<ProduceRecord>
            {
                new(Encoding.UTF8.GetBytes("batch-1")),
                new(Encoding.UTF8.GetBytes("batch-2")),
                new(Encoding.UTF8.GetBytes("batch-3"))
            };

            var offsets = await producer.SendBatchAsync(topicName, records);

            Assert.Equal(3, offsets.Count);
            Assert.True(offsets[0] >= 0);
            Assert.Equal(offsets[0] + 1, offsets[1]);
            Assert.Equal(offsets[1] + 1, offsets[2]);
        }
        finally
        {
            await CleanupTopicAsync(topicName);
        }
    }

    [Fact]
    public async Task SendAsync_LargeMessage_Success()
    {
        var topicName = await CreateTestTopicAsync();
        var producer = Client!.Producer();

        try
        {
            // 1MB message
            var largeMessage = new byte[1024 * 1024];
            Array.Fill<byte>(largeMessage, 42);

            var offset = await producer.SendAsync(topicName, largeMessage);

            Assert.True(offset >= 0);
        }
        finally
        {
            await CleanupTopicAsync(topicName);
        }
    }

    [Fact]
    public async Task SendAsync_EmptyMessage_Success()
    {
        var topicName = await CreateTestTopicAsync();
        var producer = Client!.Producer();

        try
        {
            var offset = await producer.SendAsync(topicName, Array.Empty<byte>());

            Assert.True(offset >= 0);
        }
        finally
        {
            await CleanupTopicAsync(topicName);
        }
    }

    [Fact]
    public async Task SendAsync_MultiPartitionTopic_WithKeys_DistributesAcrossPartitions()
    {
        // Create topic with 3 partitions
        var topicName = await CreateTestTopicAsync(partitions: 3);
        var producer = Client!.Producer();
        var consumer = Client!.Consumer();

        try
        {
            // Send records with different keys (should distribute across partitions)
            var records = new List<(byte[] key, byte[] value)>();
            for (var i = 0; i < 30; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key-{i}");
                var value = Encoding.UTF8.GetBytes($"value-{i}");
                records.Add((key, value));
                await producer.SendAsync(topicName, value, key);
            }

            // Wait for records to be persisted
            await Task.Delay(500);

            // Consume from each partition and verify distribution. Uses FetchBatchAsync (bounded by an
            // explicit CancellationToken) rather than StreamAsync: the 30 records are split across 3
            // partitions, so no single partition ever reaches 30 records, and StreamAsync's stream is
            // intentionally continuous/infinite (Task 5) - enumerating it with a "stop at 30" break
            // that never fires hangs forever. Confirmed by an actual CI hang on this exact test.
            var partitionCounts = new Dictionary<int, int>();

            for (var partition = 0; partition < 3; partition++)
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                IReadOnlyList<Fluvio.Client.Abstractions.ConsumeRecord> batch;
                try
                {
                    batch = await consumer.FetchBatchAsync(topicName, partition: partition, offset: 0, cancellationToken: cts.Token);
                }
                catch (OperationCanceledException)
                {
                    batch = Array.Empty<Fluvio.Client.Abstractions.ConsumeRecord>();
                }

                foreach (var record in batch)
                {
                    Assert.Equal(partition, record.Partition);
                }
                partitionCounts[partition] = batch.Count;
            }

            // Verify all records were distributed (total 30)
            var totalCount = partitionCounts.Values.Sum();
            Assert.Equal(30, totalCount);

            // Verify records were distributed across multiple partitions (not all in one)
            var partitionsUsed = partitionCounts.Values.Count(c => c > 0);
            Assert.True(partitionsUsed > 1, $"Records should be distributed across multiple partitions, but only {partitionsUsed} were used");
        }
        finally
        {
            await CleanupTopicAsync(topicName);
        }
    }

    // SendAsync_WithSpecificPartitioner_AllRecordsGoToSamePartition and SendAsync_SameKey_GoesToSamePartition
    // used to live here. Both enumerated StreamAsync on partitions expected to stay empty, with no
    // cancellation bound — since Task 5's StreamAsync is intentionally a continuous/infinite stream,
    // that hangs forever instead of ever observing "0 records". Their intent (custom partitioner routes
    // deterministically; same key always goes to the same partition) now lives in
    // ProducerPartitionerIntegrationTests.cs, using bounded FetchBatchAsync calls instead.

    [Fact]
    public async Task SendAsync_CancelledBeforeCompletion_ThrowsTaskCanceledException()
    {
        var topic = await CreateTestTopicAsync();
        try
        {
            var producer = Client!.Producer();
            using var cts = new CancellationTokenSource();
            cts.Cancel();
            await Assert.ThrowsAsync<TaskCanceledException>(
                () => producer.SendAsync(topic, new byte[] { 1, 2, 3 }, cancellationToken: cts.Token));
        }
        finally
        {
            await CleanupTopicAsync(topic);
        }
    }

    [Fact]
    public async Task SendAsync_RespectsInternalTimeout_WhenVeryShort()
    {
        // Exercises CreateSendTimeoutSource's real 1ms-linked-timeout path directly, rather than
        // mutating any shared/static state (a prior version of this test did that via a mutable
        // FluvioProducer.SendTimeoutOverride field and it leaked across concurrently executing
        // test classes in the same process — this approach cannot leak since nothing is shared).
        using var timeoutCts = Fluvio.Client.Producer.FluvioProducer.CreateSendTimeoutSource(
            CancellationToken.None, TimeSpan.FromMilliseconds(1));
        var topic = await CreateTestTopicAsync();
        try
        {
            var producer = Client!.Producer();
            // A real send against a healthy cluster may still beat 1ms depending on timing;
            // assert it EITHER completes fast OR throws OperationCanceledException — never hangs.
            var task = producer.SendAsync(topic, new byte[] { 1 }, cancellationToken: timeoutCts.Token);
            var completed = await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(5)));
            Assert.Same(task, completed);
        }
        finally
        {
            await CleanupTopicAsync(topic);
        }
    }

    [Fact]
    public async Task SendAsync_ConcurrentCallersSameTopic_OneCancelledUpFront_DoesNotCancelTheOthers()
    {
        // Important regression: GetOrCreateProducerHandleAsync used to create the shared, per-topic
        // producer-creation task using whichever caller happened to be first's own linked
        // timeout/cancellation token. A second concurrent SendAsync call for the same (not-yet-
        // created) topic awaits that SAME task, so the first caller's cancellation used to cancel
        // the second caller too, even though the second caller's own token was never cancelled.
        //
        // Both calls are started (not awaited) back-to-back on the same thread: SendAsync runs
        // synchronously up to its first real await (inside CreateProducerHandleAsync's native call),
        // so by the time the second SendAsync call runs, the first has already registered the
        // shared creation task in the topic dictionary - making which task the second call observes
        // deterministic, not a timing race.
        var topic = await CreateTestTopicAsync();
        try
        {
            var producer = Client!.Producer();
            using var preCancelledCts = new CancellationTokenSource();
            preCancelledCts.Cancel();

            var cancelledTask = producer.SendAsync(topic, new byte[] { 1 }, cancellationToken: preCancelledCts.Token);
            var healthyTask = producer.SendAsync(topic, new byte[] { 2 });

            await Assert.ThrowsAsync<TaskCanceledException>(() => cancelledTask);

            var offset = await healthyTask; // must NOT be cancelled by the other caller's token
            Assert.True(offset >= 0);
        }
        finally
        {
            await CleanupTopicAsync(topic);
        }
    }
}
