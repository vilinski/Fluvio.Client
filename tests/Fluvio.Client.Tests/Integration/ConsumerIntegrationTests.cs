using System.Text;
using Fluvio.Client.Abstractions;

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
            for (var i = 0; i < 5; i++)
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

    // Folded in from the now-deleted StreamingConsumerTests.cs: `StreamingConsumer` the class no
    // longer exists (Task 5 replaced it with the pull-based FFI-backed `StreamAsync` above), but
    // these assertions about the public `IFluvioConsumer.StreamAsync` contract still apply.

    [Fact]
    public async Task StreamAsync_ShouldStreamRecordsWithZeroPollingDelay()
    {
        var topicName = await CreateTestTopicAsync();
        var messageCount = 10;
        var producer = Client!.Producer();

        try
        {
            for (var i = 0; i < messageCount; i++)
            {
                await producer.SendAsync(topicName, Encoding.UTF8.GetBytes($"Message {i}"));
            }

            await Task.Delay(500);

            var consumer = Client!.Consumer();
            var records = new List<ConsumeRecord>();
            var startTime = DateTime.UtcNow;

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

            await foreach (var record in consumer.StreamAsync(topicName, 0, 0, cts.Token))
            {
                records.Add(record);

                if (records.Count >= messageCount)
                {
                    break;
                }
            }

            var elapsed = DateTime.UtcNow - startTime;

            Assert.Equal(messageCount, records.Count);

            for (var i = 0; i < messageCount; i++)
            {
                var message = Encoding.UTF8.GetString(records[i].Value.Span);
                Assert.Equal($"Message {i}", message);
            }

            Assert.True(elapsed.TotalMilliseconds < 1000,
                $"Streaming took too long: {elapsed.TotalMilliseconds}ms (expected < 1000ms)");
        }
        finally
        {
            await CleanupTopicAsync(topicName);
        }
    }

    [Fact]
    public async Task StreamAsync_WithExistingMessages_StreamsImmediately()
    {
        var topicName = await CreateTestTopicAsync();
        var producer = Client!.Producer();
        var consumer = Client!.Consumer();

        try
        {
            for (var i = 0; i < 10; i++)
            {
                await producer.SendAsync(topicName, Encoding.UTF8.GetBytes($"Message {i}"));
            }

            await Task.Delay(500);

            var records = new List<ConsumeRecord>();
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

            await foreach (var record in consumer.StreamAsync(topicName, 0, 0, cts.Token))
            {
                records.Add(record);

                if (records.Count >= 10)
                    break;
            }

            Assert.Equal(10, records.Count);
            for (var i = 0; i < 10; i++)
            {
                var message = Encoding.UTF8.GetString(records[i].Value.Span);
                Assert.Equal($"Message {i}", message);
            }
        }
        finally
        {
            await CleanupTopicAsync(topicName);
        }
    }

    [Fact]
    public async Task StreamAsync_ShouldHandleBackpressure()
    {
        var topicName = await CreateTestTopicAsync();
        var messageCount = 200;
        var producer = Client!.Producer();

        try
        {
            for (var i = 0; i < messageCount; i++)
            {
                await producer.SendAsync(topicName, Encoding.UTF8.GetBytes($"Backpressure {i}"));
            }

            await Task.Delay(1000);

            var consumer = Client!.Consumer();
            var records = new List<ConsumeRecord>();

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

            await foreach (var record in consumer.StreamAsync(topicName, 0, 0, cts.Token))
            {
                records.Add(record);

                if (records.Count % 10 == 0)
                {
                    await Task.Delay(50, cts.Token);
                }

                if (records.Count >= messageCount)
                    break;
            }

            Assert.Equal(messageCount, records.Count);
            Assert.False(cts.Token.IsCancellationRequested, "Operation shouldn't be cancelled or timed out.");
        }
        finally
        {
            await CleanupTopicAsync(topicName);
        }
    }

    [Fact]
    public async Task StreamAsync_ShouldHandleEmptyTopic()
    {
        var topicName = await CreateTestTopicAsync();
        var consumer = Client!.Consumer();

        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
            var records = new List<ConsumeRecord>();
            var cancelled = false;

            try
            {
                await foreach (var record in consumer.StreamAsync(topicName, 0, 0, cts.Token))
                {
                    records.Add(record);
                }
            }
            catch (OperationCanceledException)
            {
                cancelled = true;
            }

            Assert.Empty(records);
            Assert.True(cts.Token.IsCancellationRequested);
            Assert.True(cancelled);
        }
        finally
        {
            await CleanupTopicAsync(topicName);
        }
    }

    [Fact]
    public async Task CommitOffsetAsync_AtProducedOffset_RoundTripsThroughFetchLastOffset()
    {
        // Fluvio's only offset-commit mechanism (ConsumerStream::offset_commit()) commits based on
        // stream READ POSITION, not an arbitrary value — there is no API to persist a commit for an
        // offset with no record there yet (confirmed against the fluvio 0.50.1 source: offset_commit
        // commits whatever the stream has most recently read via .next()). So "commit" here means
        // "the offset of the last record I actually consumed", matching OffsetResolver.ResolveStartOffset's
        // existing `storedOffset + 1` resume convention. Commit at a real, already-produced offset.
        var topic = await CreateTestTopicAsync();
        try
        {
            var producer = Client!.Producer();
            var offset = await producer.SendAsync(topic, new byte[] { 1 });
            await producer.FlushAsync();

            var consumer = Client!.Consumer();
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await consumer.CommitOffsetAsync("test-consumer", topic, 0, offset, cts.Token);

            var stored = await consumer.FetchLastOffsetAsync("test-consumer", topic, 0, cts.Token);
            Assert.Equal(offset, stored);
        }
        finally
        {
            await CleanupTopicAsync(topic);
        }
    }

    [Fact]
    public async Task StreamAsync_ResumesFromStoredOffset_WaitsForNewRecordsInsteadOfReplaying()
    {
        var topic = await CreateTestTopicAsync();
        try
        {
            var producer = Client!.Producer();
            var lastOffset = await producer.SendAsync(topic, new byte[] { 1 });
            await producer.FlushAsync();

            var consumerGroup = $"resume-test-group-{Guid.NewGuid():N}";
            // StreamAsync resolves its own consumer identity as the group name directly (not through
            // OffsetResolver.GetConsumerId, which appends a random per-call suffix that would never
            // match across separate calls) — commit under that same identity here so the resume below
            // actually finds it.
            using var commitCts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            // Commit AT lastOffset (the record that exists), not lastOffset + 1 — see the comment on
            // CommitOffsetAsync_AtProducedOffset_RoundTripsThroughFetchLastOffset above for why.
            // ResolveStartOffset then resumes at storedOffset + 1 = lastOffset + 1, i.e. after it.
            await Client!.Consumer(new ConsumerOptions(ConsumerGroup: consumerGroup))
                .CommitOffsetAsync(consumerGroup, topic, 0, lastOffset, commitCts.Token);

            var options = new ConsumerOptions(ConsumerGroup: consumerGroup, OffsetReset: OffsetResetStrategy.StoredOrLatest);
            var consumer = Client!.Consumer(options);
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
            var received = new List<ConsumeRecord>();
            try
            {
                await foreach (var record in consumer.StreamAsync(topic, cancellationToken: cts.Token))
                {
                    received.Add(record);
                }
            }
            catch (OperationCanceledException) { /* expected: no new records within 3s */ }

            Assert.Empty(received); // must NOT have replayed the already-committed record
        }
        finally
        {
            await CleanupTopicAsync(topic);
        }
    }
}
