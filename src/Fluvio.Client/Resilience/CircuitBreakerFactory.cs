using System;
using System.IO;
using System.Net.Sockets;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.CircuitBreaker;

namespace Fluvio.Client.Resilience;

/// <summary>
/// Factory for creating circuit breaker policies to prevent cascading failures
/// </summary>
internal static class CircuitBreakerFactory
{
    /// <summary>
    /// Creates a circuit breaker policy for connection operations.
    /// Opens circuit after consecutive failures, preventing further attempts until recovery period.
    /// </summary>
    /// <param name="failureThreshold">Number of consecutive failures before opening circuit (default: 5)</param>
    /// <param name="durationOfBreak">How long to keep circuit open before attempting recovery (default: 30s)</param>
    /// <param name="logger">Optional logger for circuit breaker events</param>
    /// <returns>Async circuit breaker policy</returns>
    public static AsyncCircuitBreakerPolicy CreatePolicy(
        int failureThreshold = 5,
        TimeSpan? durationOfBreak = null,
        ILogger? logger = null)
    {
        var breakDuration = durationOfBreak ?? TimeSpan.FromSeconds(30);

        if (failureThreshold <= 0)
            throw new ArgumentException("Failure threshold must be positive", nameof(failureThreshold));

        if (breakDuration <= TimeSpan.Zero)
            throw new ArgumentException("Break duration must be positive", nameof(durationOfBreak));

        return Policy
            .Handle<TaskCanceledException>()
            .Or<IOException>()
            .Or<SocketException>()
            .CircuitBreakerAsync(
                exceptionsAllowedBeforeBreaking: failureThreshold,
                durationOfBreak: breakDuration,
                onBreak: (exception, duration) =>
                {
                    logger?.LogWarning(
                        "Circuit breaker OPENED due to {ExceptionType}: {Message}. Will remain open for {Duration}s",
                        exception.GetType().Name,
                        exception.Message,
                        duration.TotalSeconds);
                },
                onReset: () =>
                {
                    logger?.LogInformation("Circuit breaker RESET - connections will be retried");
                },
                onHalfOpen: () =>
                {
                    logger?.LogInformation("Circuit breaker HALF-OPEN - testing connection recovery");
                });
    }

    /// <summary>
    /// Creates a more aggressive circuit breaker policy for admin operations.
    /// Opens faster due to SC connection sensitivity.
    /// </summary>
    /// <param name="failureThreshold">Number of consecutive failures (default: 3)</param>
    /// <param name="durationOfBreak">Break duration (default: 60s)</param>
    /// <param name="logger">Optional logger</param>
    /// <returns>Async circuit breaker policy</returns>
    public static AsyncCircuitBreakerPolicy CreateAdminPolicy(
        int failureThreshold = 3,
        TimeSpan? durationOfBreak = null,
        ILogger? logger = null)
    {
        var breakDuration = durationOfBreak ?? TimeSpan.FromSeconds(60);

        return Policy
            .Handle<TaskCanceledException>()
            .Or<IOException>()
            .Or<SocketException>()
            .CircuitBreakerAsync(
                exceptionsAllowedBeforeBreaking: failureThreshold,
                durationOfBreak: breakDuration,
                onBreak: (exception, duration) =>
                {
                    logger?.LogWarning(
                        "Admin circuit breaker OPENED due to {ExceptionType}. SC may be overloaded. Open for {Duration}s",
                        exception.GetType().Name,
                        duration.TotalSeconds);
                },
                onReset: () =>
                {
                    logger?.LogInformation("Admin circuit breaker RESET - SC operations will be retried");
                },
                onHalfOpen: () =>
                {
                    logger?.LogInformation("Admin circuit breaker HALF-OPEN - testing SC connection");
                });
    }

    /// <summary>
    /// Wraps retry policy with circuit breaker for comprehensive resilience.
    /// Circuit breaker prevents overwhelming failed services, retry handles transient issues.
    /// </summary>
    public static IAsyncPolicy CreateResilientPolicy(
        int retryCount,
        TimeSpan retryBaseDelay,
        int circuitBreakerThreshold,
        TimeSpan circuitBreakerDuration,
        ILogger? logger = null)
    {
        var retryPolicy = RetryPolicyFactory.CreatePolicy(retryCount, retryBaseDelay, logger);
        var circuitBreaker = CreatePolicy(circuitBreakerThreshold, circuitBreakerDuration, logger);

        // Wrap retry inside circuit breaker - circuit breaker evaluates first
        return Policy.WrapAsync(circuitBreaker, retryPolicy);
    }
}
