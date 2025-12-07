using System.Diagnostics;
using Fluvio.Client;
using Fluvio.Client.Abstractions;

/*
 * Record Headers Example
 *
 * This example demonstrates how to use record headers for:
 * - Distributed tracing with W3C Trace Context
 * - Correlation IDs for request tracking
 * - Custom application metadata
 *
 * Run: dotnet run --project examples/HeadersExample
 */

Console.WriteLine("=== Fluvio C# Client - Record Headers Example ===\n");

// Configure distributed tracing (OpenTelemetry)
using var activitySource = new ActivitySource("FluvioHeadersExample");

// Connect to Fluvio (uses active profile from ~/.fluvio/config)
var client = await FluvioClient.ConnectAsync();

Console.WriteLine("Connected to Fluvio cluster\n");

// Create topic
var topic = $"hdr{Guid.NewGuid():N}"[..12];
try
{
    await client.Admin().CreateTopicAsync(topic, new TopicSpec(Partitions: 1));
    Console.WriteLine($"Created topic: {topic}\n");

    // ===== EXAMPLE 1: Simple Custom Headers =====
    Console.WriteLine("Example 1: Simple Custom Headers");
    Console.WriteLine("-----------------------------------");

    Dictionary<string, ReadOnlyMemory<byte>> simpleHeaders = new()
    {
        ["app-name"] = "my-application"u8.ToArray(),
        ["version"] = "1.0.0"u8.ToArray(),
        ["environment"] = "production"u8.ToArray()
    };

    var producer = client.Producer();
    await producer.SendBatchAsync(topic,
    [
        new ProduceRecord(
            Value: "Message with custom headers"u8.ToArray(),
            Headers: simpleHeaders)
    ]);

    var consumer = client.Consumer();
    var records = await consumer.FetchBatchAsync(topic, offset: 0);

    Console.WriteLine($"Received {records.Count} records:");
    foreach (var record in records)
    {
        if (record.Headers != null)
        {
            Console.WriteLine($"  Headers:");
            foreach (var (key, value) in record.Headers)
            {
                var stringValue = System.Text.Encoding.UTF8.GetString(value.Span);
                Console.WriteLine($"    {key}: {stringValue}");
            }
        }
    }

    Console.WriteLine();

    // ===== EXAMPLE 2: Correlation IDs =====
    Console.WriteLine("Example 2: Correlation IDs for Request Tracking");
    Console.WriteLine("-----------------------------------------------");

    var correlationId = Guid.NewGuid().ToString();
    var headersWithCorrelation = RecordHeaders.WithCorrelationId(correlationId)
        .Add("request-id", Guid.NewGuid().ToString())
        .Add("user-id", "user-123");

    await producer.SendBatchAsync(topic, [new ProduceRecord(
        Value: "Request with correlation ID"u8.ToArray(),
        Headers: headersWithCorrelation
    )]);

    records = await consumer.FetchBatchAsync(topic, offset: 1);
    var receivedRecord = records[0];

    Console.WriteLine($"Original Correlation ID: {correlationId}");
    Console.WriteLine($"Received Correlation ID: {receivedRecord.Headers.GetCorrelationId()}");
    Console.WriteLine($"Request ID: {receivedRecord.Headers.GetString("request-id")}");
    Console.WriteLine($"User ID: {receivedRecord.Headers.GetString("user-id")}");

    Console.WriteLine();

    // ===== EXAMPLE 3: Distributed Tracing with W3C Trace Context =====
    Console.WriteLine("Example 3: Distributed Tracing (W3C Trace Context)");
    Console.WriteLine("---------------------------------------------------");

    // Simulate a producer operation with distributed tracing
    Dictionary<string, ReadOnlyMemory<byte>> traceHeaders;
    {
        using var producerActivity = activitySource.StartActivity("ProcessOrder", ActivityKind.Producer);
        producerActivity?.SetTag("order.id", "ORD-12345");
        producerActivity?.SetTag("order.amount", 99.99);

        // Create headers with trace context
        traceHeaders = RecordHeaders.WithDistributedContext("order-correlation-id")
            .Add("source-service", "order-service")
            .Add("order-id", "ORD-12345");

        await producer.SendBatchAsync(topic, [new ProduceRecord(
            Value: "Order placed: ORD-12345"u8.ToArray(),
            Headers: traceHeaders
        )]);

        Console.WriteLine($"Producer Trace ID: {producerActivity?.TraceId}");
        Console.WriteLine($"Producer Span ID: {producerActivity?.SpanId}");
        Console.WriteLine($"Trace Parent: {traceHeaders.GetTraceParent()}");
    }

    // Simulate a consumer operation that continues the trace
    records = await consumer.FetchBatchAsync(topic, offset: 2);
    var orderRecord = records[0];

    // Create activity link to connect consumer with producer
    var activityLink = orderRecord.Headers.CreateActivityLink();
    {
        using var consumerActivity = activitySource.StartActivity("ProcessOrderMessage", ActivityKind.Consumer);

        if (activityLink.HasValue)
        {
            // Link consumer activity to producer activity for distributed tracing
            Console.WriteLine($"\nConsumer Trace ID: {consumerActivity?.TraceId}");
            Console.WriteLine($"Consumer Span ID: {consumerActivity?.SpanId}");
            Console.WriteLine($"Linked to Producer Trace: {activityLink.Value.Context.TraceId}");
            Console.WriteLine($"Linked to Producer Span: {activityLink.Value.Context.SpanId}");
        }

        var orderId = orderRecord.Headers.GetString("order-id");
        var correlationIdReceived = orderRecord.Headers.GetCorrelationId();

        Console.WriteLine($"\nProcessing order: {orderId}");
        Console.WriteLine($"Correlation ID: {correlationIdReceived}");
        Console.WriteLine($"Source Service: {orderRecord.Headers.GetString("source-service")}");

        consumerActivity?.SetTag("order.id", orderId);
        consumerActivity?.SetStatus(ActivityStatusCode.Ok);
    }

    Console.WriteLine();

    // ===== EXAMPLE 4: Binary Headers =====
    Console.WriteLine("Example 4: Binary Headers");
    Console.WriteLine("-------------------------");

    byte[] binaryData = [0x01, 0x02, 0x03, 0xFF, 0xFE];
    var binaryHeaders = new Dictionary<string, ReadOnlyMemory<byte>>
    {
        ["binary-token"] = binaryData,
        ["content-type"] = "application/octet-stream"u8.ToArray()
    };

    await producer.SendBatchAsync(topic, [new ProduceRecord(
        Value: "Binary header demo"u8.ToArray(),
        Headers: binaryHeaders
    )]);

    records = await consumer.FetchBatchAsync(topic, offset: 3);
    var binaryRecord = records[0];

    var receivedBinary = binaryRecord.Headers.GetBytes("binary-token");
    Console.WriteLine($"Binary header received: {BitConverter.ToString(receivedBinary!.Value.ToArray())}");
    Console.WriteLine($"Content-Type: {binaryRecord.Headers.GetString("content-type")}");

    Console.WriteLine();

    // ===== EXAMPLE 5: Integration with OpenTelemetry =====
    Console.WriteLine("Example 5: OpenTelemetry Integration Pattern");
    Console.WriteLine("---------------------------------------------");

    Console.WriteLine("""
        To integrate with OpenTelemetry:

        1. Add packages:
           - OpenTelemetry.Exporter.Console
           - OpenTelemetry.Instrumentation.Http

        2. Configure tracing:
           var tracerProvider = Sdk.CreateTracerProviderBuilder()
               .AddSource("FluvioHeadersExample")
               .AddConsoleExporter()
               .Build();

        3. Producer side:
           var headers = RecordHeaders.WithDistributedContext(correlationId);
           await producer.SendAsync(topic, data, headers: headers);

        4. Consumer side:
           var link = record.Headers.CreateActivityLink();
           using var activity = activitySource.StartActivity("process");
           activity?.AddLink(link.Value);
           // Process message with full trace context

        This enables distributed tracing across your entire system!
        """);

    Console.WriteLine("\n=== All examples completed successfully! ===");
}
finally
{
    // Cleanup
    try
    {
        await client.Admin().DeleteTopicAsync(topic);
        Console.WriteLine($"\nCleaned up topic: {topic}");
    }
    catch
    {
        // Ignore cleanup errors
    }

    await client.DisposeAsync();
}
