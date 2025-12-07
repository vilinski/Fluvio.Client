using System.Diagnostics;
using Fluvio.Client.Abstractions;

namespace Fluvio.Client.Tests.Headers;

public class RecordHeadersTests
{
    [Fact]
    public void WithTraceContext_NoActiveActivity_ReturnsEmptyHeaders()
    {
        // Arrange
        Activity.Current = null;

        // Act
        var headers = RecordHeaders.WithTraceContext();

        // Assert
        Assert.NotNull(headers);
        Assert.Empty(headers);
    }

    [Fact]
    public void WithTraceContext_ActiveActivity_ReturnsTraceParentHeader()
    {
        // Arrange
        using var activity = new Activity("test-operation").Start();

        // Act
        var headers = RecordHeaders.WithTraceContext();

        // Assert
        Assert.NotNull(headers);
        Assert.True(headers.ContainsKey(RecordHeaders.StandardHeaders.TraceParent));

        var traceParent = headers.GetString(RecordHeaders.StandardHeaders.TraceParent);
        Assert.NotNull(traceParent);

        // W3C Trace Context format: {version}-{trace-id}-{parent-id}-{trace-flags}
        var parts = traceParent.Split('-');
        Assert.Equal(4, parts.Length);
        Assert.Equal("00", parts[0]); // Version
        Assert.Equal(activity.TraceId.ToString(), parts[1]);
        Assert.Equal(activity.SpanId.ToString(), parts[2]);
    }

    [Fact]
    public void WithTraceContext_ActivityWithTraceState_ReturnsTraceStateHeader()
    {
        // Arrange
        using var activity = new Activity("test-operation");
        activity.TraceStateString = "vendor1=value1,vendor2=value2";
        activity.Start();

        // Act
        var headers = RecordHeaders.WithTraceContext();

        // Assert
        Assert.True(headers.ContainsKey(RecordHeaders.StandardHeaders.TraceState));
        var traceState = headers.GetString(RecordHeaders.StandardHeaders.TraceState);
        Assert.Equal("vendor1=value1,vendor2=value2", traceState);
    }

    [Fact]
    public void WithCorrelationId_NoId_GeneratesNewGuid()
    {
        // Act
        var headers = RecordHeaders.WithCorrelationId();

        // Assert
        Assert.NotNull(headers);
        Assert.True(headers.ContainsKey(RecordHeaders.StandardHeaders.CorrelationId));

        var correlationId = headers.GetCorrelationId();
        Assert.NotNull(correlationId);
        Assert.True(Guid.TryParse(correlationId, out _));
    }

    [Fact]
    public void WithCorrelationId_ProvidedId_UsesProvidedId()
    {
        // Arrange
        var expectedId = "test-correlation-id-123";

        // Act
        var headers = RecordHeaders.WithCorrelationId(expectedId);

        // Assert
        var correlationId = headers.GetCorrelationId();
        Assert.Equal(expectedId, correlationId);
    }

    [Fact]
    public void WithDistributedContext_IncludesTraceContextAndCorrelationId()
    {
        // Arrange
        using var activity = new Activity("test-operation").Start();
        var expectedCorrelationId = "test-correlation-id";

        // Act
        var headers = RecordHeaders.WithDistributedContext(expectedCorrelationId);

        // Assert
        Assert.True(headers.ContainsKey(RecordHeaders.StandardHeaders.TraceParent));
        Assert.True(headers.ContainsKey(RecordHeaders.StandardHeaders.CorrelationId));

        var correlationId = headers.GetCorrelationId();
        Assert.Equal(expectedCorrelationId, correlationId);

        var traceParent = headers.GetTraceParent();
        Assert.NotNull(traceParent);
        Assert.Contains(activity.TraceId.ToString(), traceParent);
    }

    [Fact]
    public void Add_StringValue_AddsUtf8EncodedHeader()
    {
        // Arrange
        var headers = new Dictionary<string, ReadOnlyMemory<byte>>();

        // Act
        headers.Add("custom-header", "test-value");

        // Assert
        Assert.True(headers.ContainsKey("custom-header"));
        var value = headers.GetString("custom-header");
        Assert.Equal("test-value", value);
    }

    [Fact]
    public void Add_BinaryValue_AddsBinaryHeader()
    {
        // Arrange
        var headers = new Dictionary<string, ReadOnlyMemory<byte>>();
        var binaryData = new byte[] { 0x01, 0x02, 0x03, 0x04 };

        // Act
        headers.Add("binary-header", binaryData);

        // Assert
        Assert.True(headers.ContainsKey("binary-header"));
        var value = headers.GetBytes("binary-header");
        Assert.NotNull(value);
        Assert.Equal(binaryData, value.Value.ToArray());
    }

    [Fact]
    public void GetString_ExistingHeader_ReturnsValue()
    {
        // Arrange
        var headers = new Dictionary<string, ReadOnlyMemory<byte>>
        {
            ["test-key"] = System.Text.Encoding.UTF8.GetBytes("test-value")
        };

        // Act
        var value = headers.GetString("test-key");

        // Assert
        Assert.Equal("test-value", value);
    }

    [Fact]
    public void GetString_NonExistingHeader_ReturnsNull()
    {
        // Arrange
        var headers = new Dictionary<string, ReadOnlyMemory<byte>>();

        // Act
        var value = headers.GetString("non-existing");

        // Assert
        Assert.Null(value);
    }

    [Fact]
    public void GetString_NullHeaders_ReturnsNull()
    {
        // Arrange
        IReadOnlyDictionary<string, ReadOnlyMemory<byte>>? headers = null;

        // Act
        var value = headers.GetString("any-key");

        // Assert
        Assert.Null(value);
    }

    [Fact]
    public void GetBytes_ExistingHeader_ReturnsValue()
    {
        // Arrange
        var expectedBytes = new byte[] { 0x01, 0x02, 0x03 };
        var headers = new Dictionary<string, ReadOnlyMemory<byte>>
        {
            ["binary-key"] = expectedBytes
        };

        // Act
        var value = headers.GetBytes("binary-key");

        // Assert
        Assert.NotNull(value);
        Assert.Equal(expectedBytes, value.Value.ToArray());
    }

    [Fact]
    public void GetBytes_NonExistingHeader_ReturnsNull()
    {
        // Arrange
        var headers = new Dictionary<string, ReadOnlyMemory<byte>>();

        // Act
        var value = headers.GetBytes("non-existing");

        // Assert
        Assert.Null(value);
    }

    [Fact]
    public void GetCorrelationId_ExistingHeader_ReturnsId()
    {
        // Arrange
        var headers = RecordHeaders.WithCorrelationId("test-id");

        // Act
        var correlationId = headers.GetCorrelationId();

        // Assert
        Assert.Equal("test-id", correlationId);
    }

    [Fact]
    public void GetTraceParent_ExistingHeader_ReturnsTraceParent()
    {
        // Arrange
        using var activity = new Activity("test").Start();
        var headers = RecordHeaders.WithTraceContext();

        // Act
        var traceParent = headers.GetTraceParent();

        // Assert
        Assert.NotNull(traceParent);
        Assert.StartsWith("00-", traceParent);
    }

    [Fact]
    public void CreateActivityLink_ValidTraceContext_ReturnsActivityLink()
    {
        // Arrange
        using var sourceActivity = new Activity("source").Start();
        var headers = RecordHeaders.WithTraceContext();

        // Act
        var link = headers.CreateActivityLink();

        // Assert
        Assert.NotNull(link);
        Assert.Equal(sourceActivity.TraceId, link.Value.Context.TraceId);
        Assert.Equal(sourceActivity.SpanId, link.Value.Context.SpanId);
    }

    [Fact]
    public void CreateActivityLink_NoTraceContext_ReturnsNull()
    {
        // Arrange
        var headers = new Dictionary<string, ReadOnlyMemory<byte>>();

        // Act
        var link = headers.CreateActivityLink();

        // Assert
        Assert.Null(link);
    }

    [Fact]
    public void CreateActivityLink_InvalidTraceContext_ReturnsNull()
    {
        // Arrange
        var headers = new Dictionary<string, ReadOnlyMemory<byte>>
        {
            [RecordHeaders.StandardHeaders.TraceParent] = System.Text.Encoding.UTF8.GetBytes("invalid-format")
        };

        // Act
        var link = headers.CreateActivityLink();

        // Assert
        Assert.Null(link);
    }

    [Fact]
    public void StandardHeaders_Constants_HaveExpectedValues()
    {
        // Assert
        Assert.Equal("traceparent", RecordHeaders.StandardHeaders.TraceParent);
        Assert.Equal("tracestate", RecordHeaders.StandardHeaders.TraceState);
        Assert.Equal("correlation-id", RecordHeaders.StandardHeaders.CorrelationId);
        Assert.Equal("message-id", RecordHeaders.StandardHeaders.MessageId);
        Assert.Equal("source", RecordHeaders.StandardHeaders.Source);
        Assert.Equal("timestamp", RecordHeaders.StandardHeaders.Timestamp);
        Assert.Equal("content-type", RecordHeaders.StandardHeaders.ContentType);
    }

    [Fact]
    public void FluentApi_ChainMultipleAdds_WorksCorrectly()
    {
        // Arrange
        var headers = new Dictionary<string, ReadOnlyMemory<byte>>();

        // Act
        headers
            .Add("header1", "value1")
            .Add("header2", "value2")
            .Add("header3", new byte[] { 0x01, 0x02 });

        // Assert
        Assert.Equal(3, headers.Count);
        Assert.Equal("value1", headers.GetString("header1"));
        Assert.Equal("value2", headers.GetString("header2"));
        Assert.Equal(new byte[] { 0x01, 0x02 }, headers.GetBytes("header3")?.ToArray());
    }

    [Fact]
    public void CompleteWorkflow_ProducerToConsumer_PreservesHeaders()
    {
        // Arrange - Producer creates message with headers
        using var activity = new Activity("producer-operation").Start();
        var producerHeaders = RecordHeaders
            .WithDistributedContext("correlation-123")
            .Add("source", "test-service")
            .Add("content-type", "application/json");

        // Act - Simulate sending and receiving
        // (Headers would be serialized via Batch.cs and deserialized via FluvioConsumer.cs)

        // Consumer receives message
        IReadOnlyDictionary<string, ReadOnlyMemory<byte>> consumerHeaders = producerHeaders;

        // Assert - Consumer can read all headers
        Assert.Equal("correlation-123", consumerHeaders.GetCorrelationId());
        Assert.Equal("test-service", consumerHeaders.GetString("source"));
        Assert.Equal("application/json", consumerHeaders.GetString("content-type"));

        var traceParent = consumerHeaders.GetTraceParent();
        Assert.NotNull(traceParent);
        Assert.Contains(activity.TraceId.ToString(), traceParent);

        // Consumer can create activity link for distributed tracing
        var link = consumerHeaders.CreateActivityLink();
        Assert.NotNull(link);
        Assert.Equal(activity.TraceId, link.Value.Context.TraceId);
    }
}
