using Fluvio.Client.Protocol;
using Xunit;

namespace Fluvio.Client.Tests.Protocol;

public class FluvioBinaryReaderTests
{
    [Fact]
    public void ReadInt8_ReadsCorrectValue()
    {
        var data = new byte[] { 127, 128 };
        using var reader = new FluvioBinaryReader(data);

        Assert.Equal(127, reader.ReadInt8());
        Assert.Equal(-128, reader.ReadInt8());
    }

    [Fact]
    public void ReadInt16_ReadsBigEndian()
    {
        var data = new byte[] { 0x01, 0x02 };
        using var reader = new FluvioBinaryReader(data);

        Assert.Equal(0x0102, reader.ReadInt16());
    }

    [Fact]
    public void ReadInt32_ReadsBigEndian()
    {
        var data = new byte[] { 0x01, 0x02, 0x03, 0x04 };
        using var reader = new FluvioBinaryReader(data);

        Assert.Equal(0x01020304, reader.ReadInt32());
    }

    [Fact]
    public void ReadInt64_ReadsBigEndian()
    {
        var data = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 };
        using var reader = new FluvioBinaryReader(data);

        Assert.Equal(0x0102030405060708, reader.ReadInt64());
    }

    [Fact]
    public void ReadString_ReadsLengthPrefixedUtf8()
    {
        var data = new byte[] { 0, 5, (byte)'h', (byte)'e', (byte)'l', (byte)'l', (byte)'o' };
        using var reader = new FluvioBinaryReader(data);

        Assert.Equal("hello", reader.ReadString());
    }

    [Fact]
    public void ReadString_NegativeOne_ReturnsNull()
    {
        var data = new byte[] { 0xFF, 0xFF };
        using var reader = new FluvioBinaryReader(data);

        Assert.Null(reader.ReadString());
    }

    [Fact]
    public void ReadBytes_ReadsLengthPrefixedData()
    {
        var data = new byte[] { 0, 0, 0, 5, 1, 2, 3, 4, 5 };
        using var reader = new FluvioBinaryReader(data);

        var result = reader.ReadBytes();
        Assert.Equal(5, result.Length);
        Assert.Equal(1, result[0]);
        Assert.Equal(5, result[4]);
    }

    [Fact]
    public void ReadNullableBytes_NegativeOne_ReturnsNull()
    {
        var data = new byte[] { 0xFF, 0xFF, 0xFF, 0xFF };
        using var reader = new FluvioBinaryReader(data);

        Assert.Null(reader.ReadNullableBytes());
    }

    [Fact]
    public void ReadBool_ReadsZeroOrOne()
    {
        var data = new byte[] { 1, 0 };
        using var reader = new FluvioBinaryReader(data);

        Assert.True(reader.ReadBool());
        Assert.False(reader.ReadBool());
    }

    [Fact]
    public void RoundTrip_AllTypes()
    {
        // Write
        using var writer = new FluvioBinaryWriter();
        writer.WriteInt8(42);
        writer.WriteInt16(1000);
        writer.WriteInt32(100000);
        writer.WriteInt64(10000000000);
        writer.WriteString("test");
        writer.WriteBytes(new byte[] { 1, 2, 3 });
        writer.WriteBool(true);

        var bytes = writer.ToArray();

        // Read
        using var reader = new FluvioBinaryReader(bytes);
        Assert.Equal(42, reader.ReadInt8());
        Assert.Equal(1000, reader.ReadInt16());
        Assert.Equal(100000, reader.ReadInt32());
        Assert.Equal(10000000000, reader.ReadInt64());
        Assert.Equal("test", reader.ReadString());
        Assert.Equal(new byte[] { 1, 2, 3 }, reader.ReadBytes());
        Assert.True(reader.ReadBool());
    }

    [Fact]
    public void EndOfStream_ThrowsException()
    {
        var data = new byte[] { 1 };
        using var reader = new FluvioBinaryReader(data);

        reader.ReadInt8();
        Assert.Throws<EndOfStreamException>(() => reader.ReadInt8());
    }
}
