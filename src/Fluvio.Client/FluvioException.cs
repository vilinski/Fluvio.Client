namespace Fluvio.Client;

/// <summary>
/// Exception thrown by Fluvio client operations
/// </summary>
public class FluvioException : Exception
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
