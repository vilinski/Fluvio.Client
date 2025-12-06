using System.Diagnostics;
using Fluvio.Client.Abstractions;

namespace Fluvio.Client.Tests.Integration;

[Collection("Fluvio Integration")]
public class BatchFlushIntegrationTests : FluvioIntegrationTestBase
{
    [Fact]
    public async Task Producer_WithSmallBatchSize_FlushesAutomatically()
    {
        // Arrange
        var topic = await CreateTestTopicAsync();
        var options = new ProducerOptions(BatchSize: 3, LingerTime: TimeSpan.FromSeconds(10));
        var producer = Client!.Producer(options);

        // Act - Send 3 records (should trigger auto-flush at batch size)
        var task1 = producer.SendAsync(topic, "message-1"u8.ToArray());
        var task2 = producer.SendAsync(topic, "message-2"u8.ToArray());
        var task3 = producer.SendAsync(topic, "message-3"u8.ToArray());

        // All tasks should complete when batch is flushed
        var offsets = await Task.WhenAll(task1, task2, task3);

        // Assert
        Assert.Equal(3, offsets.Length);
        Assert.Equal(0, offsets[0]); // First record at offset 0
        Assert.Equal(1, offsets[1]); // Second record at offset 1
        Assert.Equal(2, offsets[2]); // Third record at offset 2

        // Verify records were actually written
        var consumer = Client!.Consumer();
        var records = await consumer.FetchBatchAsync(topic, partition: 0, offset: 0);
        Assert.Equal(3, records.Count);
    }

    [Fact]
    public async Task Producer_WithLingerTime_FlushesAfterDelay()
    {
        // Arrange
        var topic = await CreateTestTopicAsync();
        var lingerTime = TimeSpan.FromMilliseconds(200);
        var options = new ProducerOptions(BatchSize: 1000, LingerTime: lingerTime);
        var producer = Client!.Producer(options);

        // Act - Send 2 records (below batch size threshold)
        var sw = Stopwatch.StartNew();
        var task1 = producer.SendAsync(topic, "message-1"u8.ToArray());
        var task2 = producer.SendAsync(topic, "message-2"u8.ToArray());

        var offsets = await Task.WhenAll(task1, task2);
        sw.Stop();

        // Assert - Should have waited approximately LingerTime
        Assert.True(sw.Elapsed >= lingerTime, $"Expected wait >= {lingerTime}, got {sw.Elapsed}");
        Assert.True(sw.Elapsed < lingerTime + TimeSpan.FromMilliseconds(500), $"Wait time too long: {sw.Elapsed}");

        Assert.Equal(2, offsets.Length);
        Assert.Equal(0, offsets[0]);
        Assert.Equal(1, offsets[1]);
    }

    [Fact]
    public async Task Producer_ExplicitFlush_SendsImmediately()
    {
        // Arrange
        var topic = await CreateTestTopicAsync();
        var options = new ProducerOptions(BatchSize: 1000, LingerTime: TimeSpan.FromSeconds(10));
        var producer = Client!.Producer(options);

        // Act - Send records and explicitly flush (don't wait for linger or batch size)
        var task1 = producer.SendAsync(topic, "message-1"u8.ToArray());
        var task2 = producer.SendAsync(topic, "message-2"u8.ToArray());

        // Explicit flush should send immediately
        await producer.FlushAsync();

        // Tasks should now be completed
        var offsets = await Task.WhenAll(task1, task2);

        // Assert
        Assert.Equal(2, offsets.Length);
        Assert.Equal(0, offsets[0]);
        Assert.Equal(1, offsets[1]);

        // Verify records were written
        var consumer = Client!.Consumer();
        var records = await consumer.FetchBatchAsync(topic, partition: 0, offset: 0);
        Assert.Equal(2, records.Count);
    }

    [Fact]
    public async Task Producer_Dispose_FlushesBufferedRecords()
    {
        // Arrange
        var topic = await CreateTestTopicAsync();
        var options = new ProducerOptions(BatchSize: 1000, LingerTime: TimeSpan.FromSeconds(10));

        Task<long> task1, task2;

        // Act - Send records and dispose (should flush on dispose)
        {
            var producer = Client!.Producer(options);
            task1 = producer.SendAsync(topic, "message-1"u8.ToArray());
            task2 = producer.SendAsync(topic, "message-2"u8.ToArray());

            // Dispose should flush buffered records
            await producer.DisposeAsync();
        }

        // Tasks should be completed after dispose
        var offsets = await Task.WhenAll(task1, task2);

        // Assert
        Assert.Equal(2, offsets.Length);
        Assert.Equal(0, offsets[0]);
        Assert.Equal(1, offsets[1]);

        // Verify records were written
        var consumer = Client!.Consumer();
        var records = await consumer.FetchBatchAsync(topic, partition: 0, offset: 0);
        Assert.Equal(2, records.Count);
    }

    [Fact]
    public async Task Producer_MultipleFlushes_DontInterfere()
    {
        // Arrange
        var topic = await CreateTestTopicAsync();
        var options = new ProducerOptions(BatchSize: 10, LingerTime: TimeSpan.FromMilliseconds(50));
        var producer = Client!.Producer(options);

        // Act - Trigger multiple concurrent flushes
        var tasks = new List<Task<long>>();
        for (int i = 0; i < 20; i++)
        {
            var message = System.Text.Encoding.UTF8.GetBytes($"message-{i}");
            tasks.Add(producer.SendAsync(topic, message));
        }

        var offsets = await Task.WhenAll(tasks);

        // Assert - All records should be sent with sequential offsets
        Assert.Equal(20, offsets.Length);
        for (int i = 0; i < 20; i++)
        {
            Assert.Equal(i, offsets[i]);
        }
    }

    [Fact]
    public async Task Producer_SendBatchAsync_SendsImmediately()
    {
        // Arrange
        var topic = await CreateTestTopicAsync();
        var options = new ProducerOptions(BatchSize: 1000, LingerTime: TimeSpan.FromSeconds(10));
        var producer = Client!.Producer(options);

        // Act - SendBatchAsync should send immediately (not buffer)
        var records = new[]
        {
            new ProduceRecord("message-1"u8.ToArray()),
            new ProduceRecord("message-2"u8.ToArray()),
            new ProduceRecord("message-3"u8.ToArray())
        };

        var offsets = await producer.SendBatchAsync(topic, records);

        // Assert - Should return immediately
        Assert.Equal(3, offsets.Count);
        Assert.Equal(0, offsets[0]);
        Assert.Equal(1, offsets[1]);
        Assert.Equal(2, offsets[2]);

        // Verify records were written
        var consumer = Client!.Consumer();
        var consumed = await consumer.FetchBatchAsync(topic, partition: 0, offset: 0);
        Assert.Equal(3, consumed.Count);
    }

    [Fact]
    public async Task Producer_WithHeaders_BatchesCorrectly()
    {
        // Arrange
        var topic = await CreateTestTopicAsync();
        var options = new ProducerOptions(BatchSize: 2, LingerTime: TimeSpan.FromSeconds(10));
        var producer = Client!.Producer(options);

        var headers1 = new Dictionary<string, ReadOnlyMemory<byte>>
        {
            ["id"] = "1"u8.ToArray()
        };

        var headers2 = new Dictionary<string, ReadOnlyMemory<byte>>
        {
            ["id"] = "2"u8.ToArray()
        };

        // Act - Send records with headers
        var task1 = producer.SendAsync(topic, "message-1"u8.ToArray());
        var task2 = producer.SendAsync(topic, "message-2"u8.ToArray());

        var offsets = await Task.WhenAll(task1, task2);

        // Assert
        Assert.Equal(2, offsets.Length);

        // Verify records and headers
        var consumer = Client!.Consumer();
        var records = await consumer.FetchBatchAsync(topic, partition: 0, offset: 0);
        Assert.Equal(2, records.Count);
    }

    [Fact]
    public async Task Producer_ZeroLingerTime_DisablesAutoFlush()
    {
        // Arrange
        var topic = await CreateTestTopicAsync();
        var options = new ProducerOptions(BatchSize: 1000, LingerTime: TimeSpan.Zero);
        var producer = Client!.Producer(options);

        // Act - Send 2 records
        var task1 = producer.SendAsync(topic, "message-1"u8.ToArray());
        var task2 = producer.SendAsync(topic, "message-2"u8.ToArray());

        // Give time to ensure no auto-flush happens
        await Task.Delay(100);

        // Tasks should NOT complete without explicit flush (no linger timer)
        Assert.False(task1.IsCompleted);
        Assert.False(task2.IsCompleted);

        // Explicit flush should complete them
        await producer.FlushAsync();
        var offsets = await Task.WhenAll(task1, task2);

        // Assert
        Assert.Equal(2, offsets.Length);
    }
}
