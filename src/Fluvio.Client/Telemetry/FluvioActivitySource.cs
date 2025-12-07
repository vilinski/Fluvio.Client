using System.Diagnostics;

namespace Fluvio.Client.Telemetry;

/// <summary>
/// Central ActivitySource for Fluvio distributed tracing.
/// Integrates with OpenTelemetry for end-to-end observability.
/// </summary>
internal static class FluvioActivitySource
{
    /// <summary>
    /// ActivitySource name following OpenTelemetry conventions.
    /// </summary>
    public const string SourceName = "Fluvio.Client";

    /// <summary>
    /// Version matches assembly version.
    /// </summary>
    public const string Version = "0.1.0";

    /// <summary>
    /// The ActivitySource instance for creating activities (spans).
    /// </summary>
    public static readonly ActivitySource Instance = new(SourceName, Version);

    /// <summary>
    /// Semantic conventions for Fluvio-specific tags.
    /// </summary>
    public static class Tags
    {
        // Fluvio-specific
        public const string Topic = "fluvio.topic";
        public const string Partition = "fluvio.partition";
        public const string CorrelationId = "fluvio.correlation_id";
        public const string Offset = "fluvio.offset";
        public const string RecordCount = "fluvio.record_count";
        public const string ConsumerGroup = "fluvio.consumer_group";
        public const string SmartModuleCount = "fluvio.smartmodule_count";

        // Connection
        public const string EndpointType = "fluvio.endpoint_type"; // SPU or SC
        public const string Endpoint = "fluvio.endpoint";
        public const string UseTls = "fluvio.use_tls";

        // Operation
        public const string Operation = "fluvio.operation"; // produce, consume, admin
        public const string ApiKey = "fluvio.api_key";

        // Standard OpenTelemetry
        public const string MessagingSystem = "messaging.system";
        public const string MessagingDestination = "messaging.destination";
        public const string MessagingOperation = "messaging.operation";
    }

    /// <summary>
    /// Activity kinds for different operations.
    /// </summary>
    public static class Operations
    {
        public const string Produce = "produce";
        public const string Consume = "consume";
        public const string StreamConsume = "stream_consume";
        public const string Admin = "admin";
        public const string Connect = "connect";
        public const string Request = "request";
    }
}
