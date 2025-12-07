namespace Fluvio.Client.Abstractions;

/// <summary>
/// SmartModule kind - the type of transformation to apply.
/// Based on Rust: fluvio-spu-schema/src/server/smartmodule.rs
/// </summary>
public enum SmartModuleKindType : byte
{
    /// <summary>
    /// Filter records (keep or discard)
    /// </summary>
    Filter = 0,

    /// <summary>
    /// Map records (one-to-one transformation)
    /// </summary>
    Map = 1,

    /// <summary>
    /// Array map (one-to-many transformation) - min_version = 15
    /// </summary>
    ArrayMap = 2,

    /// <summary>
    /// Aggregate records (stateful transformation)
    /// </summary>
    Aggregate = 3,

    /// <summary>
    /// Filter and map combined - min_version = 15
    /// </summary>
    FilterMap = 4,

    /// <summary>
    /// Generic SmartModule with context data - min_version = 17
    /// </summary>
    Generic = 7
}

/// <summary>
/// SmartModule invocation - defines how to run a SmartModule.
/// Based on Rust: fluvio-spu-schema/src/server/smartmodule.rs SmartModuleInvocation
/// </summary>
public class SmartModuleInvocation
{
    /// <summary>
    /// Name of pre-deployed SmartModule or path to WASM file
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// Kind of SmartModule transformation
    /// </summary>
    public SmartModuleKindType Kind { get; init; } = SmartModuleKindType.Generic;

    /// <summary>
    /// Parameters to pass to the SmartModule
    /// </summary>
    public IReadOnlyDictionary<string, string> Parameters { get; init; } = new Dictionary<string, string>();

    /// <summary>
    /// Optional accumulator initial value for Aggregate SmartModules
    /// </summary>
    public ReadOnlyMemory<byte>? Accumulator { get; init; }

    /// <summary>
    /// Optional lookback configuration (min_version = 20)
    /// </summary>
    public SmartModuleLookback? Lookback { get; init; }

    /// <summary>
    /// Ad-hoc WASM module bytes (gzip compressed). If null, Name is used as predefined SmartModule name.
    /// </summary>
    public ReadOnlyMemory<byte>? WasmModule { get; init; }
}

/// <summary>
/// Lookback configuration for SmartModules (min_version = 20)
/// Based on Rust: fluvio-smartmodule/src/input.rs Lookback
/// </summary>
public class SmartModuleLookback
{
    /// <summary>
    /// Number of records to look back
    /// </summary>
    public required ulong Last { get; init; }

    /// <summary>
    /// Maximum age of records to look back (min_version = 21)
    /// </summary>
    public TimeSpan? Age { get; init; }
}
