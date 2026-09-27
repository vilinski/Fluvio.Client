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
