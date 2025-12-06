using System.Diagnostics;
using Fluvio.Client.Abstractions;

namespace Fluvio.Client.Tests.Integration;

[Collection("Fluvio Integration")]
public class HeadersIntegrationTests : FluvioIntegrationTestBase
{
    [Fact]
    public async Task ProduceAndConsume_WithSimpleHeaders_PreservesHeaders()
    {
        // Arrange
        var topic = await CreateTestTopicAsync();

        var headers = new Dictionary<string, ReadOnlyMemory<byte>>
        {
            ["custom-header"] = System.Text.Encoding.UTF8.GetBytes("custom-value"),
            ["another-header"] = System.Text.Encoding.UTF8.GetBytes("another-value")
        };

        var record = new ProduceRecord(
            Value: System.Text.Encoding.UTF8.GetBytes("test-message"),
            Key: System.Text.Encoding.UTF8.GetBytes("test-key"),
            Headers: headers);

        // Act - Produce
        var producer = Client!.Producer();
        var offset = await producer.SendBatchAsync(topic, [record]);

        // Act - Consume
        var consumer = Client.Consumer();
        var consumed = await consumer.FetchBatchAsync(topic, partition: 0, offset: 0);

        // Assert
        Assert.Single(consumed);
        var consumedRecord = consumed[0];

        Assert.NotNull(consumedRecord.Headers);
        Assert.Equal(2, consumedRecord.Headers.Count);
        Assert.Equal("custom-value", consumedRecord.Headers.GetString("custom-header"));
        Assert.Equal("another-value", consumedRecord.Headers.GetString("another-header"));
    }

    [Fact]
    public async Task ProduceAndConsume_WithCorrelationId_PreservesCorrelationId()
    {
        // Arrange
        var topic = await CreateTestTopicAsync();

        var correlationId = "test-correlation-" + Guid.NewGuid();
        var headers = RecordHeaders.WithCorrelationId(correlationId);

        var record = new ProduceRecord(
            Value: System.Text.Encoding.UTF8.GetBytes("message-with-correlation"),
            Headers: headers);

        // Act - Produce
        var producer = Client!.Producer();
        await producer.SendBatchAsync(topic, [record]);

        // Act - Consume
        var consumer = Client!.Consumer();
        var consumed = await consumer.FetchBatchAsync(topic, partition: 0, offset: 0);

        // Assert
        Assert.Single(consumed);
        var consumedRecord = consumed[0];

        Assert.NotNull(consumedRecord.Headers);
        var receivedCorrelationId = consumedRecord.Headers.GetCorrelationId();
        Assert.Equal(correlationId, receivedCorrelationId);
    }

    [Fact]
    public async Task ProduceAndConsume_WithDistributedTraceContext_PreservesTraceContext()
    {
        // Arrange
        var topic = await CreateTestTopicAsync();

        using var activity = new Activity("test-producer-operation").Start();
        var correlationId = "trace-test-" + Guid.NewGuid();

        var headers = RecordHeaders
            .WithDistributedContext(correlationId)
            .Add("source", "integration-test")
            .Add("content-type", "text/plain");

        var record = new ProduceRecord(
            Value: System.Text.Encoding.UTF8.GetBytes("message-with-trace"),
            Headers: headers);

        // Act - Produce
        var producer = Client!.Producer();
        await producer.SendBatchAsync(topic, [record]);

        // Act - Consume
        var consumer = Client!.Consumer();
        var consumed = await consumer.FetchBatchAsync(topic, partition: 0, offset: 0);

        // Assert
        Assert.Single(consumed);
        var consumedRecord = consumed[0];

        Assert.NotNull(consumedRecord.Headers);
        Assert.Equal(4, consumedRecord.Headers.Count); // traceparent, correlation-id, source, content-type

        // Verify correlation ID
        var receivedCorrelationId = consumedRecord.Headers.GetCorrelationId();
        Assert.Equal(correlationId, receivedCorrelationId);

        // Verify trace context
        var traceParent = consumedRecord.Headers.GetTraceParent();
        Assert.NotNull(traceParent);
        Assert.Contains(activity.TraceId.ToString(), traceParent);
        Assert.Contains(activity.SpanId.ToString(), traceParent);

        // Verify custom headers
        Assert.Equal("integration-test", consumedRecord.Headers.GetString("source"));
        Assert.Equal("text/plain", consumedRecord.Headers.GetString("content-type"));

        // Verify we can create activity link
        var link = consumedRecord.Headers.CreateActivityLink();
        Assert.NotNull(link);
        Assert.Equal(activity.TraceId, link.Value.Context.TraceId);
    }

    [Fact]
    public async Task ProduceAndConsume_WithMultipleRecordsWithHeaders_PreservesAllHeaders()
    {
        // Arrange
        var topic = await CreateTestTopicAsync();

        var records = new List<ProduceRecord>
        {
            new(
                Value: System.Text.Encoding.UTF8.GetBytes("record-1"),
                Headers: new Dictionary<string, ReadOnlyMemory<byte>>
                {
                    ["record-id"] = System.Text.Encoding.UTF8.GetBytes("1"),
                    ["sequence"] = System.Text.Encoding.UTF8.GetBytes("first")
                }),
            new(
                Value: System.Text.Encoding.UTF8.GetBytes("record-2"),
                Headers: new Dictionary<string, ReadOnlyMemory<byte>>
                {
                    ["record-id"] = System.Text.Encoding.UTF8.GetBytes("2"),
                    ["sequence"] = System.Text.Encoding.UTF8.GetBytes("second")
                }),
            new(
                Value: System.Text.Encoding.UTF8.GetBytes("record-3"),
                Headers: null) // Record without headers
        };

        // Act - Produce
        var producer = Client!.Producer();
        await producer.SendBatchAsync(topic, records);

        // Act - Consume
        var consumer = Client!.Consumer();
        var consumed = await consumer.FetchBatchAsync(topic, partition: 0, offset: 0, maxBytes: 10 * 1024 * 1024);

        // Assert
        Assert.Equal(3, consumed.Count);

        // First record
        Assert.NotNull(consumed[0].Headers);
        Assert.Equal("1", consumed[0].Headers.GetString("record-id"));
        Assert.Equal("first", consumed[0].Headers.GetString("sequence"));

        // Second record
        Assert.NotNull(consumed[1].Headers);
        Assert.Equal("2", consumed[1].Headers.GetString("record-id"));
        Assert.Equal("second", consumed[1].Headers.GetString("sequence"));

        // Third record (no headers)
        Assert.True(consumed[2].Headers == null || consumed[2].Headers?.Count == 0);
    }

    [Fact]
    public async Task ProduceAndConsume_WithBinaryHeaders_PreservesBinaryData()
    {
        // Arrange
        var topic = await CreateTestTopicAsync();

        var binaryData = new byte[] { 0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD };
        var headers = new Dictionary<string, ReadOnlyMemory<byte>>
        {
            ["binary-header"] = binaryData,
            ["text-header"] = System.Text.Encoding.UTF8.GetBytes("text-value")
        };

        var record = new ProduceRecord(
            Value: System.Text.Encoding.UTF8.GetBytes("message-with-binary"),
            Headers: headers);

        // Act - Produce
        var producer = Client!.Producer();
        await producer.SendBatchAsync(topic, [record]);

        // Act - Consume
        var consumer = Client!.Consumer();
        var consumed = await consumer.FetchBatchAsync(topic, partition: 0, offset: 0);

        // Assert
        Assert.Single(consumed);
        var consumedRecord = consumed[0];

        Assert.NotNull(consumedRecord.Headers);
        var receivedBinary = consumedRecord.Headers.GetBytes("binary-header");
        Assert.NotNull(receivedBinary);
        Assert.Equal(binaryData, receivedBinary.Value.ToArray());
        Assert.Equal("text-value", consumedRecord.Headers.GetString("text-header"));
    }

    [Fact]
    public async Task ProduceAndConsume_WithEmptyHeaders_WorksCorrectly()
    {
        // Arrange
        var topic = await CreateTestTopicAsync();

        var record = new ProduceRecord(
            Value: System.Text.Encoding.UTF8.GetBytes("message-without-headers"),
            Headers: null);

        // Act - Produce
        var producer = Client!.Producer();
        await producer.SendBatchAsync(topic, [record]);

        // Act - Consume
        var consumer = Client!.Consumer();
        var consumed = await consumer.FetchBatchAsync(topic, partition: 0, offset: 0);

        // Assert
        Assert.Single(consumed);
        var consumedRecord = consumed[0];
        Assert.True(consumedRecord.Headers == null || consumedRecord.Headers.Count == 0);
    }

    [Fact]
    public async Task ProduceAndConsume_WithManyHeaders_PreservesAllHeaders()
    {
        // Arrange
        var topic = await CreateTestTopicAsync();

        // Create many headers
        var headers = new Dictionary<string, ReadOnlyMemory<byte>>();
        for (int i = 0; i < 20; i++)
        {
            headers[$"header-{i:D3}"] = System.Text.Encoding.UTF8.GetBytes($"value-{i}");
        }

        var record = new ProduceRecord(
            Value: System.Text.Encoding.UTF8.GetBytes("message-with-many-headers"),
            Headers: headers);

        // Act - Produce
        var producer = Client!.Producer();
        await producer.SendBatchAsync(topic, [record]);

        // Act - Consume
        var consumer = Client!.Consumer();
        var consumed = await consumer.FetchBatchAsync(topic, partition: 0, offset: 0);

        // Assert
        Assert.Single(consumed);
        var consumedRecord = consumed[0];

        Assert.NotNull(consumedRecord.Headers);
        Assert.Equal(20, consumedRecord.Headers.Count);

        // Verify all headers
        for (int i = 0; i < 20; i++)
        {
            var headerKey = $"header-{i:D3}";
            var expectedValue = $"value-{i}";
            Assert.Equal(expectedValue, consumedRecord.Headers.GetString(headerKey));
        }
    }

    [Fact]
    public async Task StreamConsumer_WithHeaders_PreservesHeaders()
    {
        // Arrange
        var topic = await CreateTestTopicAsync();

        var correlationId = "stream-test-" + Guid.NewGuid();
        var headers = RecordHeaders.WithCorrelationId(correlationId)
            .Add("test-type", "streaming");

        var record = new ProduceRecord(
            Value: System.Text.Encoding.UTF8.GetBytes("streaming-message"),
            Headers: headers);

        // Act - Produce
        var producer = Client!.Producer();
        await producer.SendBatchAsync(topic, [record]);

        // Act - Consume via streaming
        var consumer = Client!.Consumer();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        ConsumeRecord? receivedRecord = null;
        await foreach (var consumedRecord in consumer.StreamAsync(topic, partition: 0, offset: 0, cts.Token))
        {
            receivedRecord = consumedRecord;
            break; // Get first record and exit
        }

        // Assert
        Assert.NotNull(receivedRecord);
        Assert.NotNull(receivedRecord.Headers);
        Assert.Equal(correlationId, receivedRecord.Headers.GetCorrelationId());
        Assert.Equal("streaming", receivedRecord.Headers.GetString("test-type"));
    }

    [Fact]
    public async Task DistributedTracing_EndToEnd_WorksCorrectly()
    {
        // Arrange
        var topic = await CreateTestTopicAsync();

        // Producer creates trace context
        using var producerActivity = new Activity("producer-operation")
            .SetIdFormat(ActivityIdFormat.W3C)
            .Start();

        producerActivity.AddTag("test", "distributed-tracing");

        var headers = RecordHeaders.WithDistributedContext("e2e-correlation-id")
            .Add("app-name", "test-app")
            .Add("environment", "test");

        var record = new ProduceRecord(
            Value: System.Text.Encoding.UTF8.GetBytes("trace-test-message"),
            Headers: headers);

        // Act - Produce
        var producer = Client!.Producer();
        await producer.SendBatchAsync(topic, [record]);

        producerActivity.Stop();

        // Act - Consume and create consumer activity with link
        var consumer = Client!.Consumer();
        var consumed = await consumer.FetchBatchAsync(topic, partition: 0, offset: 0);

        Assert.Single(consumed);
        var consumedRecord = consumed[0];

        // Create consumer activity with link to producer
        var activityLink = consumedRecord.Headers.CreateActivityLink();
        Assert.NotNull(activityLink);

        using var consumerActivity = new Activity("consumer-operation")
            .SetIdFormat(ActivityIdFormat.W3C);

        consumerActivity.AddLink(activityLink.Value);
        consumerActivity.Start();

        // Assert - Verify trace propagation
        Assert.NotNull(consumedRecord.Headers);

        var traceParent = consumedRecord.Headers.GetTraceParent();
        Assert.NotNull(traceParent);
        Assert.Contains(producerActivity.TraceId.ToString(), traceParent);

        // Verify the activity link connects producer and consumer
        Assert.Single(consumerActivity.Links);
        Assert.Equal(producerActivity.TraceId, consumerActivity.Links.First().Context.TraceId);
        Assert.Equal(producerActivity.SpanId, consumerActivity.Links.First().Context.SpanId);

        // Verify application headers preserved
        Assert.Equal("e2e-correlation-id", consumedRecord.Headers.GetCorrelationId());
        Assert.Equal("test-app", consumedRecord.Headers.GetString("app-name"));
        Assert.Equal("test", consumedRecord.Headers.GetString("environment"));
    }
}
