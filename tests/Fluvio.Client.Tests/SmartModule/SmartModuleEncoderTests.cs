using System.Text;
using Fluvio.Client.Abstractions;
using Fluvio.Client.Protocol;
using Xunit;

namespace Fluvio.Client.Tests.SmartModule;

/// <summary>
/// Unit tests for SmartModule protocol encoding.
/// Tests verify encoding matches Rust fluvio-spu-schema implementation.
/// </summary>
public class SmartModuleEncoderTests
{
    [Fact]
    public void EncodeSmartModules_EmptyList_WritesZeroCount()
    {
        // Arrange
        using var writer = new FluvioBinaryWriter();

        // Act
        writer.EncodeSmartModules(null, version: 25);

        // Assert
        var bytes = writer.ToArray();
        Assert.Equal(4, bytes.Length); // u32 count = 0
        Assert.Equal(0, bytes[0]);
        Assert.Equal(0, bytes[1]);
        Assert.Equal(0, bytes[2]);
        Assert.Equal(0, bytes[3]);
    }

    [Fact]
    public void EncodeSmartModules_PredefinedFilter_EncodesCorrectly()
    {
        // Arrange
        using var writer = new FluvioBinaryWriter();
        SmartModuleInvocation[] smartModules =
        [
            new()
            {
                Name = "my-filter",
                Kind = SmartModuleKindType.Filter
            }
        ];

        // Act
        writer.EncodeSmartModules(smartModules, version: 25);

        // Assert
        var bytes = writer.ToArray();

        using var reader = new FluvioBinaryReader(bytes);

        // Vec count
        var count = reader.ReadInt32();
        Assert.Equal(1, count);

        // SmartModuleInvocationWasm::Predefined (tag = 0)
        var wasmTag = reader.ReadInt8();
        Assert.Equal(0, wasmTag);
        var name1 = reader.ReadString();
        Assert.Equal("my-filter", name1);

        // SmartModuleKind::Filter (tag = 0)
        var kindTag = reader.ReadInt8();
        Assert.Equal(0, kindTag);

        // SmartModuleExtraParams (empty BTreeMap)
        var paramCount = reader.ReadUInt16();
        Assert.Equal(0, paramCount);

        // Lookback (None)
        var hasLookback = reader.ReadBool();
        Assert.False(hasLookback);

        // Name (Option<String>) - version 25
        var hasName = reader.ReadBool();
        Assert.True(hasName);
        var name2 = reader.ReadString();
        Assert.Equal("my-filter", name2);
    }

    [Fact]
    public void EncodeSmartModules_PredefinedMap_EncodesCorrectly()
    {
        // Arrange
        using var writer = new FluvioBinaryWriter();
        SmartModuleInvocation[] smartModules =
        [
            new()
            {
                Name = "my-map",
                Kind = SmartModuleKindType.Map
            }
        ];

        // Act
        writer.EncodeSmartModules(smartModules, version: 25);

        // Assert
        var bytes = writer.ToArray();
        using var reader = new FluvioBinaryReader(bytes);

        reader.ReadInt32(); // count
        reader.ReadInt8(); // wasm tag
        reader.ReadString(); // wasm name

        // SmartModuleKind::Map (tag = 1)
        var kindTag = reader.ReadInt8();
        Assert.Equal(1, kindTag);
    }

    [Fact]
    public void EncodeSmartModules_AggregateWithAccumulator_EncodesCorrectly()
    {
        // Arrange
        using var writer = new FluvioBinaryWriter();
        var accumulator = Encoding.UTF8.GetBytes("initial");
        SmartModuleInvocation[] smartModules =
        [
            new()
            {
                Name = "my-aggregate",
                Kind = SmartModuleKindType.Aggregate,
                Accumulator = accumulator
            }
        ];

        // Act
        writer.EncodeSmartModules(smartModules, version: 25);

        // Assert
        var bytes = writer.ToArray();
        using var reader = new FluvioBinaryReader(bytes);

        reader.ReadInt32(); // count
        reader.ReadInt8(); // wasm tag
        reader.ReadString(); // wasm name

        // SmartModuleKind::Aggregate (tag = 3)
        var kindTag = reader.ReadInt8();
        Assert.Equal(3, kindTag);

        // Accumulator (Vec<u8>)
        var accumulatorLen = reader.ReadInt32();
        Assert.Equal(7, accumulatorLen); // "initial" = 7 bytes
        var accumulatorBytes = reader.ReadRawBytes(accumulatorLen);
        Assert.Equal("initial", Encoding.UTF8.GetString(accumulatorBytes));
    }

    [Fact]
    public void EncodeSmartModules_GenericWithNoContext_EncodesCorrectly()
    {
        // Arrange
        using var writer = new FluvioBinaryWriter();
        SmartModuleInvocation[] smartModules =
        [
            new()
            {
                Name = "my-generic",
                Kind = SmartModuleKindType.Generic
            }
        ];

        // Act
        writer.EncodeSmartModules(smartModules, version: 25);

        // Assert
        var bytes = writer.ToArray();
        using var reader = new FluvioBinaryReader(bytes);

        reader.ReadInt32(); // count
        reader.ReadInt8(); // wasm tag
        reader.ReadString(); // wasm name

        // SmartModuleKind::Generic (tag = 7)
        var kindTag = reader.ReadInt8();
        Assert.Equal(7, kindTag);

        // SmartModuleContextData::None (tag = 0)
        var contextTag = reader.ReadInt8();
        Assert.Equal(0, contextTag);
    }

    [Fact]
    public void EncodeSmartModules_WithParameters_EncodesCorrectly()
    {
        // Arrange
        using var writer = new FluvioBinaryWriter();
        SmartModuleInvocation[] smartModules =
        [
            new()
            {
                Name = "my-filter",
                Kind = SmartModuleKindType.Filter,
                Parameters = new Dictionary<string, string>
                {
                    ["threshold"] = "100",
                    ["mode"] = "strict"
                }
            }
        ];

        // Act
        writer.EncodeSmartModules(smartModules, version: 25);

        // Assert
        var bytes = writer.ToArray();
        using var reader = new FluvioBinaryReader(bytes);

        reader.ReadInt32(); // count
        reader.ReadInt8(); // wasm tag
        reader.ReadString(); // wasm name
        reader.ReadInt8(); // kind tag

        // SmartModuleExtraParams (BTreeMap<String, String>)
        var paramCount = reader.ReadUInt16();
        Assert.Equal(2, paramCount);

        // Parameters should be sorted by key (BTreeMap behavior)
        var key1 = reader.ReadString();
        var value1 = reader.ReadString();
        Assert.Equal("mode", key1); // "mode" comes before "threshold" alphabetically
        Assert.Equal("strict", value1);

        var key2 = reader.ReadString();
        var value2 = reader.ReadString();
        Assert.Equal("threshold", key2);
        Assert.Equal("100", value2);
    }

    [Fact]
    public void EncodeSmartModules_WithLookback_EncodesCorrectly()
    {
        // Arrange
        using var writer = new FluvioBinaryWriter();
        SmartModuleInvocation[] smartModules =
        [
            new()
            {
                Name = "my-filter",
                Kind = SmartModuleKindType.Filter,
                Lookback = new SmartModuleLookback
                {
                    Last = 100,
                    Age = TimeSpan.FromMinutes(5)
                }
            }
        ];

        // Act
        writer.EncodeSmartModules(smartModules, version: 25);

        // Assert
        var bytes = writer.ToArray();
        using var reader = new FluvioBinaryReader(bytes);

        reader.ReadInt32(); // count
        reader.ReadInt8(); // wasm tag
        reader.ReadString(); // wasm name
        reader.ReadInt8(); // kind tag
        reader.ReadUInt16(); // param count

        // Lookback (Option<Lookback>)
        var hasLookback = reader.ReadBool();
        Assert.True(hasLookback);

        // Lookback.last (u64)
        var last = reader.ReadUInt64();
        Assert.Equal(100ul, last);

        // Lookback.age (Option<Duration>)
        var hasAge = reader.ReadBool();
        Assert.True(hasAge);

        // Duration: u64 seconds + u32 nanoseconds
        var seconds = reader.ReadUInt64();
        var nanos = reader.ReadUInt32();
        Assert.Equal(300ul, seconds); // 5 minutes = 300 seconds
        Assert.Equal(0u, nanos);
    }

    [Fact]
    public void EncodeSmartModules_ChainMultiple_EncodesCorrectly()
    {
        // Arrange
        using var writer = new FluvioBinaryWriter();
        SmartModuleInvocation[] smartModules =
        [
            new()
            {
                Name = "filter-errors",
                Kind = SmartModuleKindType.Filter
            },
            new()
            {
                Name = "map-to-json",
                Kind = SmartModuleKindType.Map
            },
            new()
            {
                Name = "filter-threshold",
                Kind = SmartModuleKindType.FilterMap
            }
        ];

        // Act
        writer.EncodeSmartModules(smartModules, version: 25);

        // Assert
        var bytes = writer.ToArray();
        using var reader = new FluvioBinaryReader(bytes);

        // Vec count should be 3
        var count = reader.ReadInt32();
        Assert.Equal(3, count);

        // Verify we can read all three SmartModules
        for (int i = 0; i < 3; i++)
        {
            reader.ReadInt8(); // wasm tag
            var name = reader.ReadString();
            Assert.NotNull(name);
            Assert.NotEmpty(name);
            reader.ReadInt8(); // kind tag
            reader.ReadUInt16(); // param count
            reader.ReadBool(); // lookback
            reader.ReadBool(); // has name
            reader.ReadString(); // name
        }
    }

    [Fact]
    public void EncodeSmartModules_AdHocWasm_EncodesWithGzipCompression()
    {
        // Arrange
        using var writer = new FluvioBinaryWriter();
        // Use larger data that will actually compress smaller
        var wasmBytes = Encoding.UTF8.GetBytes(new string('A', 1000)); // Highly compressible
        SmartModuleInvocation[] smartModules =
        [
            new()
            {
                Name = "adhoc-module.wasm",
                Kind = SmartModuleKindType.Filter,
                WasmModule = wasmBytes
            }
        ];

        // Act
        writer.EncodeSmartModules(smartModules, version: 25);

        // Assert
        var bytes = writer.ToArray();
        using var reader = new FluvioBinaryReader(bytes);

        reader.ReadInt32(); // count

        // SmartModuleInvocationWasm::AdHoc (tag = 1)
        var wasmTag = reader.ReadInt8();
        Assert.Equal(1, wasmTag);

        // Gzip-compressed WASM (Vec<u8>)
        var compressedLen = reader.ReadInt32();
        Assert.True(compressedLen > 0);
        Assert.True(compressedLen < wasmBytes.Length); // Should be compressed smaller

        var compressedBytes = reader.ReadRawBytes(compressedLen);

        // Verify it's gzip compressed (starts with gzip magic bytes)
        Assert.Equal(0x1f, compressedBytes[0]);
        Assert.Equal(0x8b, compressedBytes[1]);
    }

    [Fact]
    public void EncodeSmartModules_AllKindTypes_EncodesCorrectTags()
    {
        // Verify all SmartModuleKindType values encode to correct tags
        (SmartModuleKindType kind, byte expectedTag)[] testCases =
        [
            (SmartModuleKindType.Filter, (byte)0),
            (SmartModuleKindType.Map, (byte)1),
            (SmartModuleKindType.ArrayMap, (byte)2),
            (SmartModuleKindType.Aggregate, (byte)3),
            (SmartModuleKindType.FilterMap, (byte)4),
            (SmartModuleKindType.Generic, (byte)7)
        ];

        foreach (var (kind, expectedTag) in testCases)
        {
            using var writer = new FluvioBinaryWriter();
            SmartModuleInvocation[] smartModules =
            [
                new()
                {
                    Name = $"test-{kind}",
                    Kind = kind
                }
            ];

            writer.EncodeSmartModules(smartModules, version: 25);

            var bytes = writer.ToArray();
            using var reader = new FluvioBinaryReader(bytes);

            reader.ReadInt32(); // count
            reader.ReadInt8(); // wasm tag
            reader.ReadString(); // wasm name

            var kindTag = (byte)reader.ReadInt8();
            Assert.Equal(expectedTag, kindTag);
        }
    }
}
