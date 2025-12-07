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
