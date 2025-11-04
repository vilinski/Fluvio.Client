using Fluvio.Client.Abstractions;

namespace Fluvio.Client.Consumer;

/// <summary>
/// Helper class for resolving consumer offsets based on strategy
/// </summary>
internal static class OffsetResolver
{
    /// <summary>
    /// Special offset value indicating "end of partition" (latest)
    /// Based on Rust: const END_OFFSET: i64 = -1
    /// </summary>
    public const long EndOffset = -1;

    /// <summary>
    /// Special offset value indicating "start of partition" (earliest)
    /// Based on Rust: const BEGINNING_OFFSET: i64 = 0
    /// </summary>
    public const long BeginningOffset = 0;

    /// <summary>
    /// Resolves the starting offset based on consumer options and stored offset.
    /// </summary>
    /// <param name="storedOffset">The last committed offset for this consumer (null if none)</param>
    /// <param name="strategy">The offset reset strategy</param>
    /// <param name="explicitOffset">Explicit offset provided by user (null if using strategy)</param>
    /// <returns>The resolved offset to start consuming from</returns>
    public static long ResolveStartOffset(
        long? storedOffset,
        OffsetResetStrategy strategy,
        long? explicitOffset = null)
    {
        // Explicit offset always takes precedence
        if (explicitOffset.HasValue)
        {
            return explicitOffset.Value;
        }

        // If using stored offset strategies
        if (strategy == OffsetResetStrategy.StoredOrEarliest ||
            strategy == OffsetResetStrategy.StoredOrLatest)
        {
            if (storedOffset.HasValue)
            {
                // Resume from stored offset (next message after committed)
                return storedOffset.Value + 1;
            }

            // No stored offset - fall back to earliest or latest
            return strategy == OffsetResetStrategy.StoredOrEarliest
                ? BeginningOffset
                : EndOffset;
        }

        // Direct strategy (Earliest or Latest)
        return strategy switch
        {
            OffsetResetStrategy.Earliest => BeginningOffset,
            OffsetResetStrategy.Latest => EndOffset,
            _ => EndOffset // Default to Latest
        };
    }

    /// <summary>
    /// Gets a unique consumer ID for a consumer group.
    /// If no consumer group is specified, returns null.
    /// </summary>
    /// <param name="consumerGroup">Optional consumer group name</param>
    /// <param name="instanceId">Optional instance ID within the group</param>
    /// <returns>Consumer ID string or null</returns>
    public static string? GetConsumerId(string? consumerGroup, string? instanceId = null)
    {
        if (string.IsNullOrEmpty(consumerGroup))
        {
            return null;
        }

        // If instance ID provided, use it; otherwise generate one
        var instance = instanceId ?? Guid.NewGuid().ToString("N")[..8];
        return $"{consumerGroup}-{instance}";
    }
}
