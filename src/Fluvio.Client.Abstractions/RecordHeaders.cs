using System.Diagnostics;

namespace Fluvio.Client.Abstractions;

/// <summary>
/// Helper utilities for working with record headers, including distributed tracing support.
/// </summary>
public static class RecordHeaders
{
    /// <summary>
    /// Standard header keys for distributed tracing (following OpenTelemetry conventions)
    /// </summary>
    public static class StandardHeaders
    {
        /// <summary>
        /// W3C Trace Context traceparent header (OpenTelemetry standard)
        /// Format: {version}-{trace-id}-{parent-id}-{trace-flags}
        /// Example: 00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01
        /// </summary>
        public const string TraceParent = "traceparent";

        /// <summary>
        /// W3C Trace Context tracestate header (vendor-specific data)
        /// </summary>
        public const string TraceState = "tracestate";

        /// <summary>
        /// Correlation ID for request tracking (application-specific)
        /// </summary>
        public const string CorrelationId = "correlation-id";

        /// <summary>
        /// Message ID for deduplication
        /// </summary>
        public const string MessageId = "message-id";

        /// <summary>
        /// Source system identifier
        /// </summary>
        public const string Source = "source";

        /// <summary>
        /// Message timestamp (ISO 8601 format)
        /// </summary>
        public const string Timestamp = "timestamp";

        /// <summary>
        /// Content type of the message payload
        /// </summary>
        public const string ContentType = "content-type";
    }

    /// <summary>
    /// Creates a headers dictionary with the current Activity's trace context.
    /// If no Activity is active, returns empty dictionary.
    /// </summary>
    /// <returns>Headers dictionary with trace context</returns>
    public static Dictionary<string, ReadOnlyMemory<byte>> WithTraceContext()
    {
        var headers = new Dictionary<string, ReadOnlyMemory<byte>>();

        var activity = Activity.Current;
        if (activity != null)
        {
            // W3C Trace Context format: {version}-{trace-id}-{parent-id}-{trace-flags}
            var traceFlagsHex = ((int)activity.ActivityTraceFlags).ToString("x2");
            var traceParent = $"00-{activity.TraceId}-{activity.SpanId}-{traceFlagsHex}";
            headers[StandardHeaders.TraceParent] = System.Text.Encoding.UTF8.GetBytes(traceParent);

            // Include tracestate if present
            if (!string.IsNullOrEmpty(activity.TraceStateString))
            {
                headers[StandardHeaders.TraceState] = System.Text.Encoding.UTF8.GetBytes(activity.TraceStateString);
            }
        }

        return headers;
    }

    /// <summary>
    /// Creates a headers dictionary with a correlation ID.
    /// If no correlation ID is provided, generates a new GUID.
    /// </summary>
    /// <param name="correlationId">Correlation ID (null to generate new)</param>
    /// <returns>Headers dictionary with correlation ID</returns>
    public static Dictionary<string, ReadOnlyMemory<byte>> WithCorrelationId(string? correlationId = null)
    {
        correlationId ??= Guid.NewGuid().ToString();
        return new Dictionary<string, ReadOnlyMemory<byte>>
        {
            [StandardHeaders.CorrelationId] = System.Text.Encoding.UTF8.GetBytes(correlationId)
        };
    }

    /// <summary>
    /// Creates a headers dictionary with both trace context and correlation ID.
    /// </summary>
    /// <param name="correlationId">Correlation ID (null to generate new)</param>
    /// <returns>Headers dictionary with trace context and correlation ID</returns>
    public static Dictionary<string, ReadOnlyMemory<byte>> WithDistributedContext(string? correlationId = null)
    {
        var headers = WithTraceContext();

        correlationId ??= Guid.NewGuid().ToString();
        headers[StandardHeaders.CorrelationId] = System.Text.Encoding.UTF8.GetBytes(correlationId);

        return headers;
    }

    /// <summary>
    /// Adds a string header to the headers dictionary.
    /// </summary>
    /// <param name="headers">Headers dictionary</param>
    /// <param name="key">Header key</param>
    /// <param name="value">Header value</param>
    /// <returns>Updated headers dictionary</returns>
    public static Dictionary<string, ReadOnlyMemory<byte>> Add(
        this Dictionary<string, ReadOnlyMemory<byte>> headers,
        string key,
        string value)
    {
        headers[key] = System.Text.Encoding.UTF8.GetBytes(value);
        return headers;
    }

    /// <summary>
    /// Adds a binary header to the headers dictionary.
    /// </summary>
    /// <param name="headers">Headers dictionary</param>
    /// <param name="key">Header key</param>
    /// <param name="value">Header value</param>
    /// <returns>Updated headers dictionary</returns>
    public static Dictionary<string, ReadOnlyMemory<byte>> Add(
        this Dictionary<string, ReadOnlyMemory<byte>> headers,
        string key,
        ReadOnlyMemory<byte> value)
    {
        headers[key] = value;
        return headers;
    }

    /// <summary>
    /// Gets a string header value from the headers dictionary.
    /// Returns null if header doesn't exist.
    /// </summary>
    /// <param name="headers">Headers dictionary</param>
    /// <param name="key">Header key</param>
    /// <returns>Header value as string, or null</returns>
    public static string? GetString(
        this IReadOnlyDictionary<string, ReadOnlyMemory<byte>>? headers,
        string key)
    {
        if (headers == null || !headers.TryGetValue(key, out var value))
        {
            return null;
        }

        return System.Text.Encoding.UTF8.GetString(value.Span);
    }

    /// <summary>
    /// Gets a binary header value from the headers dictionary.
    /// Returns null if header doesn't exist.
    /// </summary>
    /// <param name="headers">Headers dictionary</param>
    /// <param name="key">Header key</param>
    /// <returns>Header value as bytes, or null</returns>
    public static ReadOnlyMemory<byte>? GetBytes(
        this IReadOnlyDictionary<string, ReadOnlyMemory<byte>>? headers,
        string key)
    {
        if (headers == null || !headers.TryGetValue(key, out var value))
        {
            return null;
        }

        return value;
    }

    /// <summary>
    /// Gets the correlation ID from headers.
    /// Returns null if not present.
    /// </summary>
    /// <param name="headers">Headers dictionary</param>
    /// <returns>Correlation ID, or null</returns>
    public static string? GetCorrelationId(
        this IReadOnlyDictionary<string, ReadOnlyMemory<byte>>? headers)
    {
        return headers.GetString(StandardHeaders.CorrelationId);
    }

    /// <summary>
    /// Gets the trace parent from headers (W3C Trace Context).
    /// Returns null if not present.
    /// </summary>
    /// <param name="headers">Headers dictionary</param>
    /// <returns>Trace parent string, or null</returns>
    public static string? GetTraceParent(
        this IReadOnlyDictionary<string, ReadOnlyMemory<byte>>? headers)
    {
        return headers.GetString(StandardHeaders.TraceParent);
    }

    /// <summary>
    /// Creates an Activity link from message headers (for distributed tracing).
    /// Returns null if no trace context is found in headers.
    /// </summary>
    /// <param name="headers">Headers dictionary</param>
    /// <returns>ActivityLink, or null if no trace context</returns>
    public static ActivityLink? CreateActivityLink(
        this IReadOnlyDictionary<string, ReadOnlyMemory<byte>>? headers)
    {
        var traceParent = headers.GetTraceParent();
        if (traceParent == null)
        {
            return null;
        }

        // Parse W3C Trace Context: {version}-{trace-id}-{parent-id}-{trace-flags}
        var parts = traceParent.Split('-');
        if (parts.Length != 4)
        {
            return null;
        }

        // Parse trace ID (32 hex chars)
        if (parts[1].Length != 32 || !IsHexString(parts[1]))
        {
            return null;
        }
        var traceId = ActivityTraceId.CreateFromString(parts[1].AsSpan());

        // Parse span ID (16 hex chars)
        if (parts[2].Length != 16 || !IsHexString(parts[2]))
        {
            return null;
        }
        var spanId = ActivitySpanId.CreateFromString(parts[2].AsSpan());

        // Parse trace flags (2 hex chars)
        var traceFlags = parts[3].Length >= 2 && int.TryParse(parts[3].Substring(0, 2), System.Globalization.NumberStyles.HexNumber, null, out var flags)
            ? (ActivityTraceFlags)flags
            : ActivityTraceFlags.None;

        var traceState = headers.GetString(StandardHeaders.TraceState);
        var context = new ActivityContext(traceId, spanId, traceFlags, traceState);

        return new ActivityLink(context);
    }

    private static bool IsHexString(string str)
    {
        foreach (var c in str)
        {
            if (!Uri.IsHexDigit(c))
            {
                return false;
            }
        }
        return true;
    }
}
