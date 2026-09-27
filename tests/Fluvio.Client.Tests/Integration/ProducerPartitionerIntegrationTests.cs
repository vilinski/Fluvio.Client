using Fluvio.Client.Abstractions;

namespace Fluvio.Client.Tests.Integration;

[Collection("Integration")]
public class ProducerPartitionerIntegrationTests : FluvioIntegrationTestBase
{
    private sealed class AlwaysPartitionZero : IPartitioner
    {
        public int SelectPartition(string topic, ReadOnlyMemory<byte>? key, ReadOnlyMemory<byte> value, PartitionerConfig config) => 0;
    }

    [Fact]
    public async Task SendAsync_WithCustomPartitioner_AllRecordsGoToSelectedPartition()
    {
        var topic = await CreateTestTopicAsync(partitions: 3);
        try
        {
            var producer = Client!.Producer(new ProducerOptions(Partitioner: new AlwaysPartitionZero()));
            for (var i = 0; i < 10; i++)
            {
                await producer.SendAsync(topic, new byte[] { (byte)i });
            }
            await producer.FlushAsync();

            var partition0 = await Client!.Consumer().FetchBatchAsync(topic, partition: 0, offset: 0);

            // Partitions 1 and 2 are expected to stay empty. Since Task 2 of this hardening plan
            // removed FetchBatchAsync's old internal per-record timeout in favor of caller-driven
            // cancellation, a no-token fetch against a genuinely empty partition now waits
            // indefinitely instead of returning quickly — an explicit short timeout is required here.
            using var emptyCts1 = new CancellationTokenSource(TimeSpan.FromSeconds(3));
            var partition1 = await FetchEmptyOrTimeoutAsync(topic, 1, emptyCts1.Token);
            using var emptyCts2 = new CancellationTokenSource(TimeSpan.FromSeconds(3));
            var partition2 = await FetchEmptyOrTimeoutAsync(topic, 2, emptyCts2.Token);

            Assert.Equal(10, partition0.Count);
            Assert.Empty(partition1);
            Assert.Empty(partition2);
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
