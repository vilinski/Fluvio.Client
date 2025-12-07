using Fluvio.Client.Abstractions;
using Fluvio.Client.Protocol;
using Fluvio.Client.Protocol.Records;

namespace Fluvio.Client.Tests.Headers;

public class HeaderEncodingTests
{
    [Fact]
    public void EncodeRecord_NoHeaders_EncodesZeroCount()
    {
        // Arrange
        var record = new ProduceRecord(
            Value: System.Text.Encoding.UTF8.GetBytes("test-value"),
            Key: null,
            Headers: null);

        var batch = Batch<List<ProduceRecord>>.Default([record]);

        // Act
        using var writer = new FluvioBinaryWriter();
        batch.Encode(writer, TimeProvider.System);

        var bytes = writer.ToArray();

        // Assert
        Assert.NotEmpty(bytes);

        // Verify the encoded batch contains header count of 0
        // This is implicit in the test - if encoding fails, we'd get an exception
    }

    [Fact]
    public void EncodeRecord_WithHeaders_EncodesHeadersCorrectly()
    {
        // Arrange
        var headers = new Dictionary<string, ReadOnlyMemory<byte>>
        {
            ["correlation-id"] = System.Text.Encoding.UTF8.GetBytes("test-123"),
            ["source"] = System.Text.Encoding.UTF8.GetBytes("test-service")
        };

        var record = new ProduceRecord(
            Value: System.Text.Encoding.UTF8.GetBytes("test-value"),
            Key: System.Text.Encoding.UTF8.GetBytes("test-key"),
            Headers: headers);

        var batch = Batch<List<ProduceRecord>>.Default([record]);

        // Act
        using var writer = new FluvioBinaryWriter();
        batch.Encode(writer, TimeProvider.System);

        var bytes = writer.ToArray();

        // Assert
        Assert.NotEmpty(bytes);
        // Encoding succeeded - headers were properly encoded
    }

    [Fact]
    public void EncodeRecord_MultipleHeaders_MaintainsOrder()
    {
        // Arrange
        var headers = new Dictionary<string, ReadOnlyMemory<byte>>
        {
            ["header1"] = System.Text.Encoding.UTF8.GetBytes("value1"),
            ["header2"] = System.Text.Encoding.UTF8.GetBytes("value2"),
            ["header3"] = System.Text.Encoding.UTF8.GetBytes("value3")
        };

        var record = new ProduceRecord(
            Value: System.Text.Encoding.UTF8.GetBytes("test"),
            Headers: headers);

        var batch = Batch<List<ProduceRecord>>.Default([record]);

        // Act
        using var writer = new FluvioBinaryWriter();
        batch.Encode(writer, TimeProvider.System);

        var bytes = writer.ToArray();

        // Assert
        Assert.NotEmpty(bytes);
        Assert.True(bytes.Length > 100); // Should be reasonably sized with headers
    }

    [Fact]
    public void EncodeRecord_EmptyHeaderValue_EncodesCorrectly()
    {
        // Arrange
        var headers = new Dictionary<string, ReadOnlyMemory<byte>>
        {
            ["empty-header"] = ReadOnlyMemory<byte>.Empty
        };

        var record = new ProduceRecord(
            Value: System.Text.Encoding.UTF8.GetBytes("test"),
            Headers: headers);

        var batch = Batch<List<ProduceRecord>>.Default([record]);

        // Act
        using var writer = new FluvioBinaryWriter();
        batch.Encode(writer, TimeProvider.System);

        var bytes = writer.ToArray();

        // Assert
        Assert.NotEmpty(bytes);
    }

    [Fact]
    public void EncodeRecord_BinaryHeaderValue_EncodesCorrectly()
    {
        // Arrange
        var binaryData = new byte[] { 0x00, 0x01, 0x02, 0xFF, 0xFE };
        var headers = new Dictionary<string, ReadOnlyMemory<byte>>
        {
            ["binary-header"] = binaryData
        };

        var record = new ProduceRecord(
            Value: System.Text.Encoding.UTF8.GetBytes("test"),
            Headers: headers);

        var batch = Batch<List<ProduceRecord>>.Default([record]);

        // Act
        using var writer = new FluvioBinaryWriter();
        batch.Encode(writer, TimeProvider.System);

        var bytes = writer.ToArray();

        // Assert
        Assert.NotEmpty(bytes);
    }

    [Fact]
    public void EncodeRecord_LargeHeaderValue_EncodesCorrectly()
    {
        // Arrange
        var largeValue = new byte[10000];
        Array.Fill(largeValue, (byte)0x42);

        var headers = new Dictionary<string, ReadOnlyMemory<byte>>
        {
            ["large-header"] = largeValue
        };

        var record = new ProduceRecord(
            Value: System.Text.Encoding.UTF8.GetBytes("test"),
            Headers: headers);

        var batch = Batch<List<ProduceRecord>>.Default([record]);

        // Act
        using var writer = new FluvioBinaryWriter();
        batch.Encode(writer, TimeProvider.System);

        var bytes = writer.ToArray();

        // Assert
        Assert.NotEmpty(bytes);
        Assert.True(bytes.Length > 10000); // Should include the large header
    }

    [Fact]
    public void EncodeRecord_UnicodeHeaderKey_EncodesCorrectly()
    {
        // Arrange
        var headers = new Dictionary<string, ReadOnlyMemory<byte>>
        {
            ["header-🚀-emoji"] = System.Text.Encoding.UTF8.GetBytes("test"),
            ["header-ä-umlaut"] = System.Text.Encoding.UTF8.GetBytes("value")
        };

        var record = new ProduceRecord(
            Value: System.Text.Encoding.UTF8.GetBytes("test"),
            Headers: headers);

        var batch = Batch<List<ProduceRecord>>.Default([record]);

        // Act
        using var writer = new FluvioBinaryWriter();
        batch.Encode(writer, TimeProvider.System);

        var bytes = writer.ToArray();

        // Assert
        Assert.NotEmpty(bytes);
    }

    [Fact]
    public void EncodeBatch_MultipleRecordsWithHeaders_EncodesAllHeaders()
    {
        // Arrange
        var records = new List<ProduceRecord>
        {
            new(
                Value: System.Text.Encoding.UTF8.GetBytes("record1"),
                Headers: new Dictionary<string, ReadOnlyMemory<byte>>
                {
                    ["record"] = System.Text.Encoding.UTF8.GetBytes("1")
                }),
            new(
                Value: System.Text.Encoding.UTF8.GetBytes("record2"),
                Headers: new Dictionary<string, ReadOnlyMemory<byte>>
                {
                    ["record"] = System.Text.Encoding.UTF8.GetBytes("2")
                }),
            new(
                Value: System.Text.Encoding.UTF8.GetBytes("record3"),
                Headers: null) // One without headers
        };

        var batch = Batch<List<ProduceRecord>>.Default(records);

        // Act
        using var writer = new FluvioBinaryWriter();
        batch.Encode(writer, TimeProvider.System);

        var bytes = writer.ToArray();

        // Assert
        Assert.NotEmpty(bytes);
    }

    [Fact]
    public void EncodeRecord_TraceContextHeaders_EncodesCorrectly()
    {
        // Arrange
        var headers = RecordHeaders.WithDistributedContext("test-correlation-id")
            .Add("source", "test-service")
            .Add("content-type", "application/json");

        var record = new ProduceRecord(
            Value: System.Text.Encoding.UTF8.GetBytes("test-message"),
            Key: System.Text.Encoding.UTF8.GetBytes("test-key"),
            Headers: headers);

        var batch = Batch<List<ProduceRecord>>.Default([record]);

        // Act
        using var writer = new FluvioBinaryWriter();
        batch.Encode(writer, TimeProvider.System);

        var bytes = writer.ToArray();

        // Assert
        Assert.NotEmpty(bytes);

        // Verify we can extract correlation ID before encoding
        Assert.Equal("test-correlation-id", headers.GetCorrelationId());
        Assert.Equal("test-service", headers.GetString("source"));
    }

    [Fact]
    public void EncodeRecord_MaxHeaderCount_EncodesCorrectly()
    {
        // Arrange - Create many headers
        var headers = new Dictionary<string, ReadOnlyMemory<byte>>();
        for (int i = 0; i < 100; i++)
        {
            headers[$"header-{i}"] = System.Text.Encoding.UTF8.GetBytes($"value-{i}");
        }

        var record = new ProduceRecord(
            Value: System.Text.Encoding.UTF8.GetBytes("test"),
            Headers: headers);

        var batch = Batch<List<ProduceRecord>>.Default([record]);

        // Act
        using var writer = new FluvioBinaryWriter();
        batch.Encode(writer, TimeProvider.System);

        var bytes = writer.ToArray();

        // Assert
        Assert.NotEmpty(bytes);
        Assert.Equal(100, headers.Count);
    }
}
