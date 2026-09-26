namespace Fluvio.Client;

/// <summary>
/// Exception thrown by Fluvio client operations
/// </summary>
public partial class FluvioException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="FluvioException"/> class with a specified error message.
    /// </summary>
    /// <param name="message">The error message.</param>
    public FluvioException(string message) : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="FluvioException"/> class with a specified error message and a reference to the inner exception that is the cause of this exception.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The inner exception.</param>
    public FluvioException(string message, Exception innerException) : base(message, innerException)
    {
    }
}

/// <summary>
/// Exception thrown when the Fluvio cluster platform version is incompatible with the client.
/// </summary>
public class IncompatiblePlatformVersionException : FluvioException
{
    /// <summary>
    /// Gets the minimum platform version required by the client.
    /// </summary>
    public string MinimumVersion { get; }

    /// <summary>
    /// Gets the actual platform version reported by the cluster.
    /// </summary>
    public string ClusterVersion { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="IncompatiblePlatformVersionException"/> class.
    /// </summary>
    /// <param name="minimumVersion">The minimum platform version required by the client.</param>
    /// <param name="clusterVersion">The actual platform version reported by the cluster.</param>
    public IncompatiblePlatformVersionException(string minimumVersion, string clusterVersion)
        : base($"Fluvio cluster platform version {clusterVersion} is not compatible. " +
               $"Client requires minimum version {minimumVersion}. " +
               $"Please upgrade your Fluvio cluster to version {minimumVersion} or later.")
    {
        MinimumVersion = minimumVersion;
        ClusterVersion = clusterVersion;
    }
}

/// <summary>
/// Exception thrown when the native FFI layer reports a connection-related failure.
/// </summary>
public class FluvioConnectionException(string message) : FluvioException(message);

/// <summary>
/// Exception thrown when the native FFI layer reports that a topic was not found.
/// </summary>
public class TopicNotFoundException(string message) : FluvioException(message);

/// <summary>
/// Exception thrown when the native FFI layer reports that a topic already exists.
/// </summary>
public class TopicAlreadyExistsException(string message) : FluvioException(message);

public partial class FluvioException
{
    internal static class Codes
    {
        internal const int Generic = 1;
        internal const int Connection = 2;
        internal const int TopicNotFound = 3;
        internal const int TopicAlreadyExists = 4;
        internal const int Cancelled = 5;
        internal const int InvalidArgument = 6;
        internal const int Unauthorized = 7;
    }

    internal static Exception FromCode(int code, string? message)
    {
        var msg = message ?? "Fluvio operation failed";
        return code switch
        {
            Codes.Connection => new FluvioConnectionException(msg),
            Codes.TopicNotFound => new TopicNotFoundException(msg),
            Codes.TopicAlreadyExists => new TopicAlreadyExistsException(msg),
            // TaskCanceledException (an OperationCanceledException subclass) rather than the
            // base type, matching the exact-type xunit assertion in the pre-existing
            // FetchBatchAsync_EmptyTopic_BlocksUntilTimeout regression test, and matching the
            // exception .NET's own Task infrastructure surfaces for a cancelled operation.
            Codes.Cancelled => new TaskCanceledException(msg),
            _ => new FluvioException(msg),
        };
    }
}
