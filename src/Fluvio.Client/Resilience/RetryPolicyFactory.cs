using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Retry;

namespace Fluvio.Client.Resilience;

/// <summary>
/// Factory for creating Polly retry policies for Fluvio operations
/// </summary>
internal static class RetryPolicyFactory
{
    /// <summary>
    /// Creates an async retry policy with exponential backoff for data operations
    /// </summary>
    /// <param name="maxRetries">Maximum number of retry attempts</param>
    /// <param name="baseDelay">Base delay for exponential backoff</param>
    /// <param name="logger">Optional logger for retry events</param>
    /// <returns>Configured retry policy</returns>
    public static AsyncRetryPolicy CreatePolicy(int maxRetries, TimeSpan baseDelay, ILogger? logger = null)
    {
        if (maxRetries <= 0)
            throw new ArgumentException("MaxRetries must be positive", nameof(maxRetries));

        if (baseDelay <= TimeSpan.Zero)
            throw new ArgumentException("BaseDelay must be positive", nameof(baseDelay));

        return Policy
            .Handle<TaskCanceledException>() // Connection/request timeouts
            .Or<IOException>()               // Network errors
            .Or<SocketException>()           // Socket-level errors
            .WaitAndRetryAsync(
                retryCount: maxRetries,
                sleepDurationProvider: retryAttempt =>
                {
                    // Exponential backoff: baseDelay * 2^(attempt-1)
                    // With max cap of 5 seconds to prevent excessive waits
                    var delay = baseDelay.TotalMilliseconds * Math.Pow(2, retryAttempt - 1);
                    return TimeSpan.FromMilliseconds(Math.Min(delay, 5000));
                },
                onRetry: (exception, timeSpan, retryCount, context) =>
                {
                    logger?.LogWarning(
                        "Retry {RetryCount}/{MaxRetries} after {DelayMs}ms due to: {ExceptionType} - {Message}",
                        retryCount, maxRetries, timeSpan.TotalMilliseconds,
                        exception.GetType().Name, exception.Message);
                });
    }

    /// <summary>
    /// Creates a retry policy for admin operations with more aggressive retry settings
    /// </summary>
    /// <param name="maxRetries">Maximum number of retry attempts</param>
    /// <param name="baseDelay">Base delay for exponential backoff</param>
    /// <param name="logger">Optional logger for retry events</param>
    /// <returns>Configured retry policy for admin operations</returns>
    public static AsyncRetryPolicy CreateAdminPolicy(int maxRetries, TimeSpan baseDelay, ILogger? logger = null)
    {
        if (maxRetries <= 0)
            throw new ArgumentException("MaxRetries must be positive", nameof(maxRetries));

        if (baseDelay <= TimeSpan.Zero)
            throw new ArgumentException("BaseDelay must be positive", nameof(baseDelay));

        return Policy
            .Handle<TaskCanceledException>()
            .Or<IOException>()
            .Or<SocketException>()
            // Note: FluvioException retry logic can be added when ErrorCode property is implemented
            .WaitAndRetryAsync(
                retryCount: maxRetries,
                sleepDurationProvider: retryAttempt =>
                {
                    // More generous backoff for admin operations
                    var delay = baseDelay.TotalMilliseconds * Math.Pow(2, retryAttempt - 1);
                    return TimeSpan.FromMilliseconds(Math.Min(delay, 10000)); // Max 10s for admin
                },
                onRetry: (exception, timeSpan, retryCount, context) =>
                {
                    logger?.LogWarning(
                        "Admin operation retry {RetryCount}/{MaxRetries} after {DelayMs}ms due to: {ExceptionType}",
                        retryCount, maxRetries, timeSpan.TotalMilliseconds, exception.GetType().Name);
                });
    }
}
