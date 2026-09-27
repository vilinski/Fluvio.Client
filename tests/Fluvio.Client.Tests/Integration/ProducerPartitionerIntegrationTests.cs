using Fluvio.Client.Abstractions;

namespace Fluvio.Client.Tests.Integration;

[Collection("Integration")]
public class ProducerPartitionerIntegrationTests : FluvioIntegrationTestBase
{
    private sealed class AlwaysPartitionTwo : IPartitioner
    {
        public int SelectPartition(string topic, ReadOnlyMemory<byte>? key, ReadOnlyMemory<byte> value, PartitionerConfig config) => 2;
    }

    [Fact]
    public async Task SendAsync_WithCustomPartitioner_AllRecordsGoToSelectedPartition()
    {
        // Regression: a partitioner that always selects partition 0 is indistinguishable from a
        // silently-ignored custom partitioner, since the native DynamicPartitioner's own fallback
        // (when no explicit partition is set) is also 0 - such a test would still pass even if the
        // explicit-partitioning wiring were completely broken. Targeting partition 2 (never the
        // fallback) is what actually proves the custom IPartitioner is consulted.
        var topic = await CreateTestTopicAsync(partitions: 3);
        try
        {
            var producer = Client!.Producer(new ProducerOptions(Partitioner: new AlwaysPartitionTwo()));
            for (var i = 0; i < 10; i++)
            {
                await producer.SendAsync(topic, new byte[] { (byte)i });
            }
            await producer.FlushAsync();

            var partition2 = await Client!.Consumer().FetchBatchAsync(topic, partition: 2, offset: 0);

            // Partitions 0 and 1 are expected to stay empty. Since Task 2 of this hardening plan
            // removed FetchBatchAsync's old internal per-record timeout in favor of caller-driven
            // cancellation, a no-token fetch against a genuinely empty partition now waits
            // indefinitely instead of returning quickly — an explicit short timeout is required here.
            using var emptyCts0 = new CancellationTokenSource(TimeSpan.FromSeconds(3));
            var partition0 = await FetchEmptyOrTimeoutAsync(topic, 0, emptyCts0.Token);
            using var emptyCts1 = new CancellationTokenSource(TimeSpan.FromSeconds(3));
            var partition1 = await FetchEmptyOrTimeoutAsync(topic, 1, emptyCts1.Token);

            Assert.Equal(10, partition2.Count);
            Assert.Empty(partition0);
            Assert.Empty(partition1);
        }
        finally
        {
            await CleanupTopicAsync(topic);
        }
    }

    [Fact]
    public async Task SendAsync_SameKey_AlwaysGoesToSamePartition()
    {
        var topic = await CreateTestTopicAsync(partitions: 3);
        try
        {
            var producer = Client!.Producer(); // default SiphashRoundRobinPartitioner
            var key = new byte[] { 0xAB, 0xCD };
            for (var i = 0; i < 10; i++)
            {
                await producer.SendAsync(topic, new byte[] { (byte)i }, key: key);
            }
            await producer.FlushAsync();

            var counts = new List<int>();
            for (var p = 0; p < 3; p++)
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
                counts.Add((await FetchEmptyOrTimeoutAsync(topic, p, cts.Token)).Count);
            }

            Assert.Single(counts, c => c == 10); // all 10 landed in exactly one partition
        }
        finally
        {
            await CleanupTopicAsync(topic);
        }
    }

    private async Task<IReadOnlyList<ConsumeRecord>> FetchEmptyOrTimeoutAsync(string topic, int partition, CancellationToken cancellationToken)
    {
        try
        {
            return await Client!.Consumer().FetchBatchAsync(topic, partition: partition, offset: 0, cancellationToken: cancellationToken);
        }
        catch (OperationCanceledException)
        {
            return Array.Empty<ConsumeRecord>();
        }
    }
}
