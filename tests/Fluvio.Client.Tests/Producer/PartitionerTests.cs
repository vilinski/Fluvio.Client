using System.Text;
using Fluvio.Client.Abstractions;
using Fluvio.Client.Producer;
using Xunit;

namespace Fluvio.Client.Tests.Producer;

public class PartitionerTests
{
    [Fact]
    public void SiphashRoundRobinPartitioner_WithoutKey_UsesRoundRobin()
    {
        // Arrange
        var partitioner = new SiphashRoundRobinPartitioner();
        var config = new PartitionerConfig(3); // 3 partitions: [0, 1, 2]
        var value = "test-value"u8.ToArray();

        // Act - select partition 9 times (3 full cycles)
        var partitions = new List<int>();
        for (var i = 0; i < 9; i++)
        {
            var partition = partitioner.SelectPartition("test-topic", null, value, config);
            partitions.Add(partition);
        }

        // Assert - should cycle through partitions 0, 1, 2, 0, 1, 2, 0, 1, 2
        Assert.Equal(new[] { 0, 1, 2, 0, 1, 2, 0, 1, 2 }, partitions);
    }

    [Fact]
    public void SiphashRoundRobinPartitioner_WithKey_ReturnsSamePartitionForSameKey()
    {
        // Arrange
        var partitioner = new SiphashRoundRobinPartitioner();
        var config = new PartitionerConfig(5); // 5 partitions
        var key = "user-123"u8.ToArray();
        var value = "test-value"u8.ToArray();

        // Act - select partition multiple times with same key
        var partition1 = partitioner.SelectPartition("test-topic", key, value, config);
        var partition2 = partitioner.SelectPartition("test-topic", key, value, config);
        var partition3 = partitioner.SelectPartition("test-topic", key, value, config);

        // Assert - should always return the same partition for the same key
        Assert.Equal(partition1, partition2);
        Assert.Equal(partition2, partition3);
        Assert.InRange(partition1, 0, 4); // partition should be in [0, 4]
    }

    [Fact]
    public void SiphashRoundRobinPartitioner_WithDifferentKeys_DistributesAcrossPartitions()
    {
        // Arrange
        var partitioner = new SiphashRoundRobinPartitioner();
        var config = new PartitionerConfig(5); // 5 partitions
        var value = "test-value"u8.ToArray();

        // Act - select partitions for 100 different keys
        var partitionCounts = new Dictionary<int, int>();
        for (var i = 0; i < 100; i++)
        {
            var key = Encoding.UTF8.GetBytes($"user-{i}");
            var partition = partitioner.SelectPartition("test-topic", key, value, config);

            if (!partitionCounts.ContainsKey(partition))
                partitionCounts[partition] = 0;
            partitionCounts[partition]++;
        }

        // Assert - should distribute across all partitions (with reasonable distribution)
        Assert.Equal(5, partitionCounts.Count); // All 5 partitions should be used
        foreach (var count in partitionCounts.Values)
        {
            // Each partition should get roughly 20 records (100/5)
            // Allow +/- 10 for hash distribution variance
            Assert.InRange(count, 10, 30);
        }
    }

    [Fact]
    public void SiphashRoundRobinPartitioner_WithSinglePartition_AlwaysReturnsZero()
    {
        // Arrange
        var partitioner = new SiphashRoundRobinPartitioner();
        var config = new PartitionerConfig(1); // Single partition
        var key = "user-123"u8.ToArray();
        var value = "test-value"u8.ToArray();

        // Act
        var partition1 = partitioner.SelectPartition("test-topic", key, value, config);
        var partition2 = partitioner.SelectPartition("test-topic", null, value, config);

        // Assert
        Assert.Equal(0, partition1);
        Assert.Equal(0, partition2);
    }

    [Fact]
    public void SiphashRoundRobinPartitioner_WithEmptyKey_UsesRoundRobin()
    {
        // Arrange
        var partitioner = new SiphashRoundRobinPartitioner();
        var config = new PartitionerConfig(3);
        var emptyKey = Array.Empty<byte>();
        var value = "test-value"u8.ToArray();

        // Act - empty key should trigger round-robin
        var partitions = new List<int>();
        for (var i = 0; i < 6; i++)
        {
            var partition = partitioner.SelectPartition("test-topic", emptyKey, value, config);
            partitions.Add(partition);
        }

        // Assert - should use round-robin for empty keys
        Assert.Equal(new[] { 0, 1, 2, 0, 1, 2 }, partitions);
    }

    [Fact]
    public void SpecificPartitioner_AlwaysReturnsSamePartition()
    {
        // Arrange
        var partitioner = new SpecificPartitioner(2);
        var config = new PartitionerConfig(5);
        var key1 = "user-123"u8.ToArray();
        var key2 = "user-456"u8.ToArray();
        var value = "test-value"u8.ToArray();

        // Act
        var partition1 = partitioner.SelectPartition("test-topic", key1, value, config);
        var partition2 = partitioner.SelectPartition("test-topic", key2, value, config);
        var partition3 = partitioner.SelectPartition("test-topic", null, value, config);

        // Assert - should always return partition 2
        Assert.Equal(2, partition1);
        Assert.Equal(2, partition2);
        Assert.Equal(2, partition3);
    }

    [Fact]
    public void SpecificPartitioner_ThrowsIfPartitionNotAvailable()
    {
        // Arrange
        var partitioner = new SpecificPartitioner(5);
        var config = new PartitionerConfig(3); // Only partitions [0, 1, 2] available
        var value = "test-value"u8.ToArray();

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() =>
            partitioner.SelectPartition("test-topic", null, value, config));

        Assert.Contains("partition 5 is not available", exception.Message);
    }

    [Fact]
    public void SpecificPartitioner_ThrowsOnNegativePartition()
    {
        // Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => new SpecificPartitioner(-1));
    }

    [Fact]
    public void PartitionerConfig_DefaultsToAllPartitions()
    {
        // Arrange & Act
        var config = new PartitionerConfig(5);

        // Assert
        Assert.Equal(5, config.PartitionCount);
        Assert.Equal(new[] { 0, 1, 2, 3, 4 }, config.AvailablePartitions);
    }

    [Fact]
    public void PartitionerConfig_CanSpecifyCustomAvailablePartitions()
    {
        // Arrange & Act
        var config = new PartitionerConfig(5, new[] { 1, 3, 4 });

        // Assert
        Assert.Equal(5, config.PartitionCount);
        Assert.Equal(new[] { 1, 3, 4 }, config.AvailablePartitions);
    }

    [Fact]
    public void SiphashRoundRobinPartitioner_ThrowsIfNoAvailablePartitions()
    {
        // Arrange
        var partitioner = new SiphashRoundRobinPartitioner();
        var config = new PartitionerConfig(3, Array.Empty<int>()); // No available partitions
        var value = "test-value"u8.ToArray();

        // Act & Assert
        Assert.Throws<InvalidOperationException>(() =>
            partitioner.SelectPartition("test-topic", null, value, config));
    }

    [Fact]
    public async Task SiphashRoundRobinPartitioner_ThreadSafe_RoundRobinDoesNotSkipPartitions()
    {
        // Arrange
        var partitioner = new SiphashRoundRobinPartitioner();
        var config = new PartitionerConfig(3);
        var value = "test-value"u8.ToArray();

        // Act - select partitions from multiple threads concurrently
        var partitions = new List<int>();
        var tasks = new List<Task>();
        var lockObj = new object();

        for (var i = 0; i < 10; i++)
        {
            tasks.Add(Task.Run(() =>
            {
                for (var j = 0; j < 10; j++)
                {
                    var partition = partitioner.SelectPartition("test-topic", null, value, config);
                    lock (lockObj)
                    {
                        partitions.Add(partition);
                    }
                }
            }));
        }

        await Task.WhenAll(tasks);

        // Assert - all partitions should be represented
        Assert.Equal(100, partitions.Count);
        Assert.Contains(0, partitions);
        Assert.Contains(1, partitions);
        Assert.Contains(2, partitions);

        // Each partition should be used roughly equally (100/3 ≈ 33)
        var partitionCounts = partitions.GroupBy(p => p).ToDictionary(g => g.Key, g => g.Count());
        foreach (var count in partitionCounts.Values)
        {
            Assert.InRange(count, 25, 40); // Allow some variance for concurrency
        }
    }
}
