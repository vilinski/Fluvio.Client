using Fluvio.Client.Protocol;

namespace Fluvio.Client.Tests.Protocol;

public class FluvioBinaryWriterTests
{
    [Fact]
    public void WriteInt8_WritesCorrectValue()
    {
        using var writer = new FluvioBinaryWriter();
        writer.WriteInt8(127);
        writer.WriteInt8(-128);

        var bytes = writer.ToArray();
        Assert.Equal(2, bytes.Length);
        Assert.Equal(127, (sbyte)bytes[0]);
        Assert.Equal(128, bytes[1]); // -128 as unsigned
    }

    [Fact]
    public void WriteInt16_WritesBigEndian()
    {
        using var writer = new FluvioBinaryWriter();
        writer.WriteInt16(0x0102);

        var bytes = writer.ToArray();
        Assert.Equal(2, bytes.Length);
        Assert.Equal(0x01, bytes[0]);
        Assert.Equal(0x02, bytes[1]);
    }

    [Fact]
    public void WriteInt32_WritesBigEndian()
    {
        using var writer = new FluvioBinaryWriter();
        writer.WriteInt32(0x01020304);

        var bytes = writer.ToArray();
        Assert.Equal(4, bytes.Length);
        Assert.Equal(0x01, bytes[0]);
        Assert.Equal(0x02, bytes[1]);
        Assert.Equal(0x03, bytes[2]);
        Assert.Equal(0x04, bytes[3]);
    }

    [Fact]
    public void WriteInt64_WritesBigEndian()
    {
        using var writer = new FluvioBinaryWriter();
        writer.WriteInt64(0x0102030405060708);

        var bytes = writer.ToArray();
        Assert.Equal(8, bytes.Length);
        Assert.Equal(0x01, bytes[0]);
        Assert.Equal(0x08, bytes[7]);
    }

    [Fact]
    public void WriteString_WritesLengthPrefixedUtf8()
    {
        using var writer = new FluvioBinaryWriter();
        writer.WriteString("hello");

        var bytes = writer.ToArray();
        Assert.Equal(7, bytes.Length); // 2 bytes length + 5 bytes string
        Assert.Equal(0, bytes[0]);
        Assert.Equal(5, bytes[1]);
        Assert.Equal((byte)'h', bytes[2]);
        Assert.Equal((byte)'o', bytes[6]);
    }

    [Fact]
    public void WriteString_Null_WritesNegativeOne()
    {
        using var writer = new FluvioBinaryWriter();
        writer.WriteString(null);

        var bytes = writer.ToArray();
        Assert.Equal(2, bytes.Length);
        Assert.Equal(0xFF, bytes[0]);
        Assert.Equal(0xFF, bytes[1]);
    }

    [Fact]
    public void WriteBytes_WritesLengthPrefixedData()
    {
        using var writer = new FluvioBinaryWriter();
        var data = new byte[] { 1, 2, 3, 4, 5 };
        writer.WriteBytes(data);

        var bytes = writer.ToArray();
        Assert.Equal(9, bytes.Length); // 4 bytes length + 5 bytes data
        Assert.Equal(5, bytes[3]); // Length in big-endian
        Assert.Equal(1, bytes[4]);
        Assert.Equal(5, bytes[8]);
    }

    [Fact]
    public void WriteNullableBytes_Null_WritesNegativeOne()
    {
        using var writer = new FluvioBinaryWriter();
        writer.WriteNullableBytes(null);

        var bytes = writer.ToArray();
        Assert.Equal(4, bytes.Length);
        Assert.Equal(0xFF, bytes[0]);
        Assert.Equal(0xFF, bytes[3]);
    }

    [Fact]
    public void WriteBool_WritesZeroOrOne()
    {
        using var writer = new FluvioBinaryWriter();
        writer.WriteBool(true);
        writer.WriteBool(false);

        var bytes = writer.ToArray();
        Assert.Equal(2, bytes.Length);
        Assert.Equal(1, bytes[0]);
        Assert.Equal(0, bytes[1]);
    }

    [Fact]
    public void WriteVarInt_PositiveNumber()
    {
        using var writer = new FluvioBinaryWriter();
        writer.WriteVarInt(150);

        var bytes = writer.ToArray();
        Assert.True(bytes.Length > 0);
    }

    [Fact]
    public void WriteVarInt_NegativeNumber()
    {
        using var writer = new FluvioBinaryWriter();
        writer.WriteVarInt(-150);

        var bytes = writer.ToArray();
        Assert.True(bytes.Length > 0);
    }
}
