namespace Fluvio.Client;

/// <summary>
/// Exception thrown by Fluvio client operations
/// </summary>
public class FluvioException : Exception
{
    public FluvioException(string message) : base(message)
    {
    }

    public FluvioException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
