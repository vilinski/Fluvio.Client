using System.Diagnostics.Metrics;

namespace Fluvio.Client.Telemetry;

/// <summary>
/// Provides metrics instrumentation for Fluvio client operations.
/// Uses System.Diagnostics.Metrics for OpenTelemetry compatibility.
/// </summary>
public sealed class FluvioMetrics : IDisposable
{
    private readonly Meter _meter;

    // Connection metrics
    private readonly Counter<long> _connectionsTotal;
    private readonly Counter<long> _connectionFailuresTotal;
    private readonly UpDownCounter<long> _activeConnections;

    // Request metrics
    private readonly Counter<long> _requestsTotal;
    private readonly Counter<long> _requestFailuresTotal;
    private readonly Histogram<double> _requestDuration;

    // Producer metrics
    private readonly Counter<long> _messagesProducedTotal;
    private readonly Counter<long> _bytesProducedTotal;
    private readonly Counter<long> _produceFailuresTotal;

    // Consumer metrics
    private readonly Counter<long> _messagesConsumedTotal;
    private readonly Counter<long> _bytesConsumedTotal;
    private readonly Counter<long> _consumeFailuresTotal;

    // Circuit breaker metrics
    private readonly Counter<long> _circuitBreakerOpened;
    private readonly Counter<long> _circuitBreakerReset;

    // Retry metrics
    private readonly Counter<long> _retriesTotal;

    /// <summary>
    /// Initializes a new instance of FluvioMetrics.
    /// </summary>
    /// <param name="meterName">Name of the meter (default: "Fluvio.Client")</param>
    /// <param name="version">Version of the instrumentation (default: "1.0.0")</param>
    public FluvioMetrics(string meterName = "Fluvio.Client", string version = "1.0.0")
    {
        _meter = new Meter(meterName, version);

        // Connection metrics
        _connectionsTotal = _meter.CreateCounter<long>(
            "fluvio.client.connections.total",
            description: "Total number of connection attempts");

        _connectionFailuresTotal = _meter.CreateCounter<long>(
            "fluvio.client.connections.failures",
            description: "Total number of failed connection attempts");

        _activeConnections = _meter.CreateUpDownCounter<long>(
            "fluvio.client.connections.active",
            description: "Number of currently active connections");

        // Request metrics
        _requestsTotal = _meter.CreateCounter<long>(
            "fluvio.client.requests.total",
            description: "Total number of requests sent");

        _requestFailuresTotal = _meter.CreateCounter<long>(
            "fluvio.client.requests.failures",
            description: "Total number of failed requests");

        _requestDuration = _meter.CreateHistogram<double>(
            "fluvio.client.requests.duration",
            unit: "ms",
            description: "Request duration in milliseconds");

        // Producer metrics
        _messagesProducedTotal = _meter.CreateCounter<long>(
            "fluvio.client.producer.messages.total",
            description: "Total number of messages produced");

        _bytesProducedTotal = _meter.CreateCounter<long>(
            "fluvio.client.producer.bytes.total",
            unit: "By",
            description: "Total bytes produced");

        _produceFailuresTotal = _meter.CreateCounter<long>(
            "fluvio.client.producer.failures",
            description: "Total number of produce failures");

        // Consumer metrics
        _messagesConsumedTotal = _meter.CreateCounter<long>(
            "fluvio.client.consumer.messages.total",
            description: "Total number of messages consumed");

        _bytesConsumedTotal = _meter.CreateCounter<long>(
            "fluvio.client.consumer.bytes.total",
            unit: "By",
            description: "Total bytes consumed");

        _consumeFailuresTotal = _meter.CreateCounter<long>(
            "fluvio.client.consumer.failures",
            description: "Total number of consume failures");

        // Circuit breaker metrics
        _circuitBreakerOpened = _meter.CreateCounter<long>(
            "fluvio.client.circuitbreaker.opened",
            description: "Number of times circuit breaker opened");

        _circuitBreakerReset = _meter.CreateCounter<long>(
            "fluvio.client.circuitbreaker.reset",
            description: "Number of times circuit breaker reset");

        // Retry metrics
        _retriesTotal = _meter.CreateCounter<long>(
            "fluvio.client.retries.total",
            description: "Total number of retry attempts");
    }

    #region Connection Metrics

    /// <summary>
    /// Records a successful connection attempt.
    /// </summary>
    /// <param name="endpoint">The endpoint connected to.</param>
    /// <param name="type">The connection type.</param>
    public void RecordConnection(string endpoint, string type) =>
        _connectionsTotal.Add(1, new KeyValuePair<string, object?>("endpoint", endpoint), new KeyValuePair<string, object?>("type", type));

    /// <summary>
    /// Records a failed connection attempt.
    /// </summary>
    /// <param name="endpoint">The endpoint attempted.</param>
    /// <param name="type">The connection type.</param>
    /// <param name="reason">The reason for failure.</param>
    public void RecordConnectionFailure(string endpoint, string type, string reason) =>
        _connectionFailuresTotal.Add(1,
            new KeyValuePair<string, object?>("endpoint", endpoint),
            new KeyValuePair<string, object?>("type", type),
            new KeyValuePair<string, object?>("reason", reason));

    /// <summary>
    /// Increments the count of active connections.
    /// </summary>
    /// <param name="endpoint">The endpoint.</param>
    public void IncrementActiveConnections(string endpoint) =>
        _activeConnections.Add(1, new KeyValuePair<string, object?>("endpoint", endpoint));

    /// <summary>
    /// Decrements the count of active connections.
    /// </summary>
    /// <param name="endpoint">The endpoint.</param>
    public void DecrementActiveConnections(string endpoint) =>
        _activeConnections.Add(-1, new KeyValuePair<string, object?>("endpoint", endpoint));

    #endregion

    #region Request Metrics

    /// <summary>
    /// Records a request sent to the server.
    /// </summary>
    /// <param name="apiKey">The API key of the request.</param>
    public void RecordRequest(string apiKey) =>
        _requestsTotal.Add(1, new KeyValuePair<string, object?>("api_key", apiKey));

    /// <summary>
    /// Records a failed request.
    /// </summary>
    /// <param name="apiKey">The API key of the request.</param>
    /// <param name="reason">The reason for failure.</param>
    public void RecordRequestFailure(string apiKey, string reason) =>
        _requestFailuresTotal.Add(1,
            new KeyValuePair<string, object?>("api_key", apiKey),
            new KeyValuePair<string, object?>("reason", reason));

    /// <summary>
    /// Records the duration of a request.
    /// </summary>
    /// <param name="apiKey">The API key of the request.</param>
    /// <param name="durationMs">The duration in milliseconds.</param>
    public void RecordRequestDuration(string apiKey, double durationMs) =>
        _requestDuration.Record(durationMs, new KeyValuePair<string, object?>("api_key", apiKey));

    #endregion

    #region Producer Metrics

    /// <summary>
    /// Records messages produced to a topic.
    /// </summary>
    /// <param name="topic">The topic name.</param>
    /// <param name="messageCount">The number of messages produced.</param>
    public void RecordMessageProduced(string topic, int messageCount = 1) =>
        _messagesProducedTotal.Add(messageCount, new KeyValuePair<string, object?>("topic", topic));

    /// <summary>
    /// Records the number of bytes produced to a topic.
    /// </summary>
    /// <param name="topic">The topic name.</param>
    /// <param name="bytes">The number of bytes produced.</param>
    public void RecordBytesProduced(string topic, long bytes) =>
        _bytesProducedTotal.Add(bytes, new KeyValuePair<string, object?>("topic", topic));

    /// <summary>
    /// Records a produce failure for a topic.
    /// </summary>
    /// <param name="topic">The topic name.</param>
    /// <param name="reason">The reason for failure.</param>
    public void RecordProduceFailure(string topic, string reason) =>
        _produceFailuresTotal.Add(1,
            new KeyValuePair<string, object?>("topic", topic),
            new KeyValuePair<string, object?>("reason", reason));

    #endregion

    #region Consumer Metrics

    /// <summary>
    /// Records messages consumed from a topic.
    /// </summary>
    /// <param name="topic">The topic name.</param>
    /// <param name="messageCount">The number of messages consumed.</param>
    public void RecordMessageConsumed(string topic, int messageCount = 1) =>
        _messagesConsumedTotal.Add(messageCount, new KeyValuePair<string, object?>("topic", topic));

    /// <summary>
    /// Records the number of bytes consumed from a topic.
    /// </summary>
    /// <param name="topic">The topic name.</param>
    /// <param name="bytes">The number of bytes consumed.</param>
    public void RecordBytesConsumed(string topic, long bytes) =>
        _bytesConsumedTotal.Add(bytes, new KeyValuePair<string, object?>("topic", topic));

    /// <summary>
    /// Records a consume failure for a topic.
    /// </summary>
    /// <param name="topic">The topic name.</param>
    /// <param name="reason">The reason for failure.</param>
    public void RecordConsumeFailure(string topic, string reason) =>
        _consumeFailuresTotal.Add(1,
            new KeyValuePair<string, object?>("topic", topic),
            new KeyValuePair<string, object?>("reason", reason));

    #endregion

    #region Circuit Breaker Metrics

    /// <summary>
    /// Records when the circuit breaker is opened for an endpoint.
    /// </summary>
    /// <param name="endpoint">The endpoint.</param>
    public void RecordCircuitBreakerOpened(string endpoint) =>
        _circuitBreakerOpened.Add(1, new KeyValuePair<string, object?>("endpoint", endpoint));

    /// <summary>
    /// Records when the circuit breaker is reset for an endpoint.
    /// </summary>
    /// <param name="endpoint">The endpoint.</param>
    public void RecordCircuitBreakerReset(string endpoint) =>
        _circuitBreakerReset.Add(1, new KeyValuePair<string, object?>("endpoint", endpoint));

    #endregion

    #region Retry Metrics

    /// <summary>
    /// Records a retry attempt for an operation.
    /// </summary>
    /// <param name="operation">The operation being retried.</param>
    /// <param name="attemptNumber">The attempt number.</param>
    public void RecordRetry(string operation, int attemptNumber) =>
        _retriesTotal.Add(1,
            new KeyValuePair<string, object?>("operation", operation),
            new KeyValuePair<string, object?>("attempt", attemptNumber));

    #endregion

    /// <summary>
    /// Disposes the metrics instrumentation.
    /// </summary>
    public void Dispose()
    {
        _meter?.Dispose();
    }
}
