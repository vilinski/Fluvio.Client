using System;
using Fluvio.Client.Abstractions;

namespace Fluvio.Client;

/// <summary>
/// Extension methods for FluvioClientOptions validation
/// </summary>
public static class FluvioClientOptionsExtensions
{
    /// <summary>
    /// Validates the Fluvio client options
    /// </summary>
    /// <param name="options">Options to validate</param>
    /// <exception cref="ArgumentException">Thrown when configuration is invalid</exception>
    public static void Validate(this FluvioClientOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.Endpoint))
            throw new ArgumentException("Endpoint cannot be null or empty", nameof(options.Endpoint));

        if (!TryParseEndpoint(options.Endpoint, out var host, out var port))
            throw new ArgumentException($"Invalid endpoint format: '{options.Endpoint}'. Expected 'host:port'", nameof(options.Endpoint));

        if (string.IsNullOrEmpty(host))
            throw new ArgumentException($"Invalid host in endpoint: '{options.Endpoint}'", nameof(options.Endpoint));

        if (port <= 0 || port > 65535)
            throw new ArgumentException($"Invalid port in endpoint: '{options.Endpoint}'. Port must be between 1 and 65535", nameof(options.Endpoint));

        if (!string.IsNullOrEmpty(options.ScEndpoint) && !TryParseEndpoint(options.ScEndpoint, out _, out _))
            throw new ArgumentException($"Invalid SC endpoint format: '{options.ScEndpoint}'. Expected 'host:port'", nameof(options.ScEndpoint));

        if (options.ConnectionTimeout <= TimeSpan.Zero)
            throw new ArgumentException("ConnectionTimeout must be positive", nameof(options.ConnectionTimeout));

        if (options.RequestTimeout <= TimeSpan.Zero)
            throw new ArgumentException("RequestTimeout must be positive", nameof(options.RequestTimeout));

        if (options.ConnectionTimeout >= options.RequestTimeout)
            throw new ArgumentException("ConnectionTimeout should be less than RequestTimeout", nameof(options.ConnectionTimeout));
    }

    private static bool TryParseEndpoint(string endpoint, out string host, out int port)
    {
        host = "";
        port = 0;

        var parts = endpoint.Split(':');
        if (parts.Length != 2)
            return false;

        host = parts[0];
        return int.TryParse(parts[1], out port);
    }
}
