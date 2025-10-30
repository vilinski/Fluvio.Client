using System.Text;
using Fluvio.Client.Abstractions;
using Xunit;

namespace Fluvio.Client.Tests.Integration;

/// <summary>
/// High-performance streaming consumer tests
/// </summary>
[Collection("Integration")]
public class StreamingConsumerTests : FluvioIntegrationTestBase
{
    [Fact]
    public async Task StreamAsync_ShouldStreamRecordsWithZeroPollingDelay()
    {
        // Arrange
        var topicName = await CreateTestTopicAsync();
        var messageCount = 10;
        var producer = Client!.Producer();

        try
        {
            // Produce test messages
            for (int i = 0; i < messageCount; i++)
            {
                await producer.SendAsync(topicName, Encoding.UTF8.GetBytes($"Message {i}"));
            }

            await Task.Delay(500); // Wait for messages to be available

            // Act - Stream records
            var consumer = Client!.Consumer();
            var records = new List<ConsumeRecord>();
            var startTime = DateTime.UtcNow;

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

            await foreach (var record in consumer.StreamAsync(topicName, 0, 0, cts.Token))
            {
                records.Add(record);

                if (records.Count >= messageCount)
                {
                    break; // Got all messages
                }
            }

            var elapsed = DateTime.UtcNow - startTime;

            // Assert
            Assert.Equal(messageCount, records.Count);

            for (int i = 0; i < messageCount; i++)
            {
                var message = Encoding.UTF8.GetString(records[i].Value.Span);
                Assert.Equal($"Message {i}", message);
            }

            // Verify streaming is fast (no 100ms polling delays)
            // With 10 messages, old polling would take ~1 second (10 * 100ms)
            // Streaming should be much faster (< 1000ms)
            Assert.True(elapsed.TotalMilliseconds < 1000,
                $"Streaming took too long: {elapsed.TotalMilliseconds}ms (expected < 1000ms)");
        }
        finally
        {
            // Cleanup
            await CleanupTopicAsync(topicName);
        }
    }

    [Fact]
    public async Task StreamAsync_ShouldHandleContinuousStream()
    {
        // Arrange
        var topicName = await CreateTestTopicAsync();
        var producer = Client!.Producer();
        var consumer = Client!.Consumer();

        try
        {
            // Start streaming before producing
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            var streamTask = Task.Run(async () =>
            {
                var records = new List<ConsumeRecord>();
                await foreach (var record in consumer.StreamAsync(topicName, 0, 0, cts.Token))
                {
                    records.Add(record);

                    if (records.Count >= 5)
                        break;
                }
                return records;
            });

            // Give stream time to establish
            await Task.Delay(1000);

            // Produce messages while streaming
            for (int i = 0; i < 5; i++)
            {
                await producer.SendAsync(topicName, Encoding.UTF8.GetBytes($"Continuous {i}"));
                await Task.Delay(100); // Small delay between produces
            }

            // Act
            var records = await streamTask;

            // Assert
            Assert.Equal(5, records.Count);
        }
        finally
        {
            await CleanupTopicAsync(topicName);
        }
    }

    [Fact]
    public async Task StreamAsync_ShouldHandleBackpressure()
    {
        // Arrange
        var topicName = await CreateTestTopicAsync();
        var messageCount = 200; // More than channel capacity (100)
        var producer = Client!.Producer();

        try
        {
            // Produce many messages
            for (int i = 0; i < messageCount; i++)
            {
                await producer.SendAsync(topicName, Encoding.UTF8.GetBytes($"Backpressure {i}"));
            }

            await Task.Delay(1000); // Wait for all messages to be available

            // Act - Stream with slow consumer
            var consumer = Client!.Consumer();
            var records = new List<ConsumeRecord>();

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

            await foreach (var record in consumer.StreamAsync(topicName, 0, 0, cts.Token))
            {
                records.Add(record);

                // Simulate slow consumer
                if (records.Count % 10 == 0)
                {
                    await Task.Delay(50); // Slow down every 10 records
                }

                if (records.Count >= messageCount)
                    break;
            }

            // Assert - All messages received despite backpressure
            Assert.Equal(messageCount, records.Count);
        }
        finally
        {
            await CleanupTopicAsync(topicName);
        }
    }

    [Fact]
    public async Task StreamAsync_ShouldHandleEmptyTopic()
    {
        // Arrange
        var topicName = await CreateTestTopicAsync();
        var consumer = Client!.Consumer();

        try
        {
            // Act - Stream from empty topic with timeout
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            var records = new List<ConsumeRecord>();

            try
            {
                await foreach (var record in consumer.StreamAsync(topicName, 0, 0, cts.Token))
                {
                    records.Add(record);
                }
            }
            catch (OperationCanceledException)
            {
                // Expected - timeout on empty topic
            }

            // Assert - No records received (topic is empty)
            Assert.Empty(records);
        }
        finally
        {
            await CleanupTopicAsync(topicName);
        }
    }
}
