using System.Text;
using Fluvio.Client.Abstractions;
using Xunit;

namespace Fluvio.Client.Tests.Integration;

/// <summary>
/// Consumer integration tests
/// </summary>
[Collection("Integration")]
public class ConsumerIntegrationTests : FluvioIntegrationTestBase
{
    [Fact]
    public async Task FetchBatchAsync_AfterProduce_ReturnsMessages()
    {
        var topicName = await CreateTestTopicAsync();
        var producer = Client!.Producer();
        var consumer = Client!.Consumer();

        try
        {
            // Produce messages
            await producer.SendAsync(topicName, Encoding.UTF8.GetBytes("msg1"));
            await producer.SendAsync(topicName, Encoding.UTF8.GetBytes("msg2"));
            await producer.SendAsync(topicName, Encoding.UTF8.GetBytes("msg3"));

            // Wait a bit for messages to be available
            await Task.Delay(500);

            // Consume messages
            var records = await consumer.FetchBatchAsync(topicName, partition: 0, offset: 0);

            Assert.True(records.Count >= 3);
            Assert.Equal("msg1", Encoding.UTF8.GetString(records[0].Value.Span));
            Assert.Equal("msg2", Encoding.UTF8.GetString(records[1].Value.Span));
            Assert.Equal("msg3", Encoding.UTF8.GetString(records[2].Value.Span));
        }
        finally
        {
            await CleanupTopicAsync(topicName);
        }
    }

    [Fact]
    public async Task FetchBatchAsync_WithKey_ReturnsKeyAndValue()
    {
        var topicName = await CreateTestTopicAsync();
        var producer = Client!.Producer();
        var consumer = Client!.Consumer();

        try
        {
            // Produce message with key
            var key = Encoding.UTF8.GetBytes("my-key");
            var value = Encoding.UTF8.GetBytes("my-value");
            await producer.SendAsync(topicName, value, key);

            // Wait for message
            await Task.Delay(500);

            // Consume
            var records = await consumer.FetchBatchAsync(topicName, partition: 0, offset: 0);

            Assert.NotEmpty(records);
            Assert.NotNull(records[0].Key);
            Assert.Equal("my-key", Encoding.UTF8.GetString(records[0].Key!.Value.Span));
            Assert.Equal("my-value", Encoding.UTF8.GetString(records[0].Value.Span));
        }
        finally
        {
            await CleanupTopicAsync(topicName);
        }
    }

    [Fact]
    public async Task FetchBatchAsync_FromSpecificOffset_ReturnsCorrectMessages()
    {
        var topicName = await CreateTestTopicAsync();
        var producer = Client!.Producer();
        var consumer = Client!.Consumer();

        try
        {
            // Produce 5 messages
            for (int i = 0; i < 5; i++)
            {
                await producer.SendAsync(topicName, Encoding.UTF8.GetBytes($"msg{i}"));
            }

            await Task.Delay(500);

            // Fetch from offset 2
            var records = await consumer.FetchBatchAsync(topicName, partition: 0, offset: 2);

            Assert.True(records.Count >= 3);
            Assert.Equal(2, records[0].Offset);
            Assert.Equal("msg2", Encoding.UTF8.GetString(records[0].Value.Span));
        }
        finally
        {
            await CleanupTopicAsync(topicName);
        }
    }

    [Fact]
    public async Task StreamAsync_ProducesAndConsumes_InRealTime()
    {
        var topicName = await CreateTestTopicAsync();
        var producer = Client!.Producer();
        var consumer = Client!.Consumer();

        try
        {
            // Produce initial messages
            await producer.SendAsync(topicName, Encoding.UTF8.GetBytes("stream1"));
            await producer.SendAsync(topicName, Encoding.UTF8.GetBytes("stream2"));

            await Task.Delay(500);

            // Start streaming
            var receivedMessages = new List<string>();
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

            var streamTask = Task.Run(async () =>
            {
                await foreach (var record in consumer.StreamAsync(topicName, 0, 0, cts.Token))
                {
                    var msg = Encoding.UTF8.GetString(record.Value.Span);
                    receivedMessages.Add(msg);

                    if (receivedMessages.Count >= 2)
                    {
                        cts.Cancel();
                        break;
                    }
                }
            });

            await streamTask;

            Assert.Contains("stream1", receivedMessages);
            Assert.Contains("stream2", receivedMessages);
        }
        finally
        {
            await CleanupTopicAsync(topicName);
        }
    }

    [Fact]
    public async Task FetchBatchAsync_EmptyTopic_BlocksUntilTimeout()
    {
        // StreamFetch is a streaming API that blocks waiting for data.
        // This test confirms the expected behavior: it should timeout when there's no data.
        var topicName = await CreateTestTopicAsync();
        var consumer = Client!.Consumer();

        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

            await Assert.ThrowsAsync<TaskCanceledException>(async () =>
            {
                await consumer.FetchBatchAsync(topicName, partition: 0, offset: 0, cancellationToken: cts.Token);
            });
        }
        finally
        {
            await CleanupTopicAsync(topicName);
        }
    }
}
