using System.IO.Compression;
using Fluvio.Client.Abstractions;

namespace Fluvio.Client.Protocol;

/// <summary>
/// Encoder for SmartModule protocol structures.
/// Based on Rust: fluvio-spu-schema/src/server/smartmodule.rs
/// Protocol version: 25 (COMMON_VERSION)
/// </summary>
internal static class SmartModuleEncoder
{
    // Version constants from Rust
    private const short COMMON_VERSION_HAS_SM_NAME = 25;
    private const short SMARTMODULE_LOOKBACK = 20;
    private const short SMARTMODULE_LOOKBACK_AGE = 21;

    /// <summary>
    /// Encodes a list of SmartModuleInvocations
    /// </summary>
    public static void EncodeSmartModules(this FluvioBinaryWriter writer, IReadOnlyList<SmartModuleInvocation>? smartModules, short version = 25)
    {
        if (smartModules == null || smartModules.Count == 0)
        {
            writer.WriteInt32(0); // Empty Vec
            return;
        }

        writer.WriteInt32(smartModules.Count);
        foreach (var sm in smartModules)
        {
            EncodeSmartModuleInvocation(writer, sm, version);
        }
    }

    /// <summary>
    /// Encodes a single SmartModuleInvocation
    /// Protocol order:
    /// 1. wasm (SmartModuleInvocationWasm enum)
    /// 2. kind (SmartModuleKind enum)
    /// 3. params (SmartModuleExtraParams)
    /// 4. name (Option&lt;String&gt;) - only if version >= 25
    /// </summary>
    private static void EncodeSmartModuleInvocation(FluvioBinaryWriter writer, SmartModuleInvocation sm, short version)
    {
        // 1. Encode wasm (SmartModuleInvocationWasm enum)
        if (sm.WasmModule.HasValue && sm.WasmModule.Value.Length > 0)
        {
            // AdHoc variant (tag = 1)
            writer.WriteInt8(1);

            // Gzip compress the WASM bytes
            var compressedWasm = GzipCompress(sm.WasmModule.Value.Span);
            writer.WriteInt32(compressedWasm.Length);
            writer._stream.Write(compressedWasm);
        }
        else
        {
            // Predefined variant (tag = 0)
            writer.WriteInt8(0);
            writer.WriteString(sm.Name);
        }

        // 2. Encode kind (SmartModuleKind enum)
        EncodeSmartModuleKind(writer, sm);

        // 3. Encode params (SmartModuleExtraParams)
        EncodeSmartModuleExtraParams(writer, sm, version);

        // 4. Encode name (Option<String>) - only if version >= 25
        if (version >= COMMON_VERSION_HAS_SM_NAME)
        {
            writer.WriteBool(true); // Some
            writer.WriteString(sm.Name);
        }
    }

    /// <summary>
    /// Encodes SmartModuleKind enum
    /// </summary>
    private static void EncodeSmartModuleKind(FluvioBinaryWriter writer, SmartModuleInvocation sm)
    {
        switch (sm.Kind)
        {
            case SmartModuleKindType.Filter:
                writer.WriteInt8(0);
                break;

            case SmartModuleKindType.Map:
                writer.WriteInt8(1);
                break;

            case SmartModuleKindType.ArrayMap:
                writer.WriteInt8(2);
                break;

            case SmartModuleKindType.Aggregate:
                writer.WriteInt8(3);
                // Write accumulator (Vec<u8>)
                if (sm.Accumulator.HasValue)
                {
                    writer.WriteInt32(sm.Accumulator.Value.Length);
                    writer._stream.Write(sm.Accumulator.Value.Span);
                }
                else
                {
                    writer.WriteInt32(0); // Empty Vec
                }
                break;

            case SmartModuleKindType.FilterMap:
                writer.WriteInt8(4);
                break;

            case SmartModuleKindType.Generic:
                writer.WriteInt8(7);
                // Write SmartModuleContextData (tag = 0 for None)
                if (sm.Accumulator.HasValue && sm.Accumulator.Value.Length > 0)
                {
                    // Aggregate context (tag = 1)
                    writer.WriteInt8(1);
                    writer.WriteInt32(sm.Accumulator.Value.Length);
                    writer._stream.Write(sm.Accumulator.Value.Span);
                }
                else
                {
                    // None context (tag = 0)
                    writer.WriteInt8(0);
                }
                break;

            default:
                throw new ArgumentException($"Unknown SmartModule kind: {sm.Kind}", nameof(sm));
        }
    }

    /// <summary>
    /// Encodes SmartModuleExtraParams
    /// Protocol order:
    /// 1. BTreeMap&lt;String, String&gt; (u16 count + key-value pairs)
    /// 2. Option&lt;Lookback&gt; (only if version >= 20)
    /// </summary>
    private static void EncodeSmartModuleExtraParams(FluvioBinaryWriter writer, SmartModuleInvocation sm, short version)
    {
        // 1. Encode parameters as BTreeMap<String, String>
        // BTreeMap uses u16 count (not u32 like Vec)
        writer.WriteUInt16((ushort)sm.Parameters.Count);
        foreach (var (key, value) in sm.Parameters.OrderBy(kv => kv.Key)) // BTreeMap is sorted
        {
            writer.WriteString(key);
            writer.WriteString(value);
        }

        // 2. Encode lookback (Option<Lookback>) - only if version >= 20
        if (version >= SMARTMODULE_LOOKBACK)
        {
            if (sm.Lookback != null)
            {
                writer.WriteBool(true); // Some

                // Encode Lookback struct
                writer.WriteUInt64(sm.Lookback.Last);

                // Encode age (Option<Duration>) - only if version >= 21
                if (version >= SMARTMODULE_LOOKBACK_AGE)
                {
                    if (sm.Lookback.Age.HasValue)
                    {
                        writer.WriteBool(true); // Some

                        // Encode Duration: u64 secs + u32 nanos
                        var totalSeconds = (ulong)sm.Lookback.Age.Value.TotalSeconds;
                        var nanos = (uint)((sm.Lookback.Age.Value.TotalSeconds - totalSeconds) * 1_000_000_000);
                        writer.WriteUInt64(totalSeconds);
                        writer.WriteUInt32(nanos);
                    }
                    else
                    {
                        writer.WriteBool(false); // None
                    }
                }
            }
            else
            {
                writer.WriteBool(false); // None
            }
        }
    }

    /// <summary>
    /// Gzip compress data for AdHoc WASM modules
    /// </summary>
    private static byte[] GzipCompress(ReadOnlySpan<byte> data)
    {
        using var outputStream = new MemoryStream();
        using (var gzipStream = new GZipStream(outputStream, CompressionLevel.Optimal))
        {
            gzipStream.Write(data);
        }
        return outputStream.ToArray();
    }
}
