using System.Text;
using System.Linq;
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

            // Consume from each partition and verify distribution
            var partitionCounts = new Dictionary<int, int>();

            for (var partition = 0; partition < 3; partition++)
            {
                var count = 0;
                await foreach (var record in consumer.StreamAsync(topicName, partition, offset: 0))
                {
                    Assert.Equal(partition, record.Partition);
                    count++;
                    if (count >= 30) break;
                }
                partitionCounts[partition] = count;
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

    [Fact]
    public async Task SendAsync_WithSpecificPartitioner_AllRecordsGoToSamePartition()
    {
        // Create topic with 3 partitions
        var topicName = await CreateTestTopicAsync(partitions: 3);

        var producerOptions = new ProducerOptions(
            Partitioner: new Fluvio.Client.Producer.SpecificPartitioner(1) // Always use partition 1
        );
        var producer = Client!.Producer(producerOptions);
        var consumer = Client!.Consumer();

        try
        {
            // Send 10 records (should all go to partition 1)
            for (var i = 0; i < 10; i++)
            {
                await producer.SendAsync(topicName, Encoding.UTF8.GetBytes($"value-{i}"));
            }

            // Wait for records to be persisted
            await Task.Delay(500);

            // Check partition 0 - should be empty
            var partition0Count = 0;
            await foreach (var _ in consumer.StreamAsync(topicName, 0, offset: 0))
            {
                partition0Count++;
                if (partition0Count >= 10) break;
            }
            Assert.Equal(0, partition0Count);

            // Check partition 1 - should have all 10 records
            var partition1Count = 0;
            await foreach (var record in consumer.StreamAsync(topicName, 1, offset: 0))
            {
                Assert.Equal(1, record.Partition);
                partition1Count++;
                if (partition1Count >= 10) break;
            }
            Assert.Equal(10, partition1Count);

            // Check partition 2 - should be empty
            var partition2Count = 0;
            await foreach (var _ in consumer.StreamAsync(topicName, 2, offset: 0))
            {
                partition2Count++;
                if (partition2Count >= 10) break;
            }
            Assert.Equal(0, partition2Count);
        }
        finally
        {
            await CleanupTopicAsync(topicName);
        }
    }

    [Fact]
    public async Task SendAsync_SameKey_GoesToSamePartition()
    {
        // Create topic with 3 partitions
        var topicName = await CreateTestTopicAsync(partitions: 3);
        var producer = Client!.Producer();
        var consumer = Client!.Consumer();

        try
        {
            var key = Encoding.UTF8.GetBytes("consistent-key");

            // Send 10 records with the same key
            for (var i = 0; i < 10; i++)
            {
                await producer.SendAsync(topicName, Encoding.UTF8.GetBytes($"value-{i}"), key);
            }

            // Wait for records to be persisted
            await Task.Delay(500);

            // Find which partition the records went to
            int? targetPartition = null;
            var recordCount = 0;

            for (var partition = 0; partition < 3; partition++)
            {
                var partitionCount = 0;
                await foreach (var record in consumer.StreamAsync(topicName, partition, offset: 0))
                {
                    partitionCount++;
                    Assert.Equal(partition, record.Partition);
                    if (partitionCount >= 10) break;
                }

                if (partitionCount > 0)
                {
                    targetPartition = partition;
                    recordCount = partitionCount;
                }
            }

            // Verify all 10 records went to the same partition
            Assert.NotNull(targetPartition);
            Assert.Equal(10, recordCount);
        }
        finally
        {
            await CleanupTopicAsync(topicName);
        }
    }
}
