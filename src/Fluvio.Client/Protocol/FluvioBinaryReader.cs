using System.Buffers.Binary;
using System.Text;

namespace Fluvio.Client.Protocol;

/// <summary>
/// Binary reader for Fluvio protocol (big-endian, Kafka-inspired)
/// </summary>
internal sealed class FluvioBinaryReader : IDisposable
{
    private readonly Stream _stream;
    private readonly byte[] _buffer;

    public FluvioBinaryReader(Stream stream)
    {
        _stream = stream;
        _buffer = new byte[8];
    }

    public FluvioBinaryReader(ReadOnlyMemory<byte> data)
        : this(new MemoryStream(data.ToArray()))
    {
    }

    public long Position => _stream.Position;
    public long Length => _stream.Length;
    public bool EndOfStream => _stream.Position >= _stream.Length;

    public sbyte ReadInt8()
    {
        var b = _stream.ReadByte();
        if (b == -1) throw new EndOfStreamException();
        return (sbyte)b;
    }

    public short ReadInt16()
    {
        ReadExactly(_buffer, 0, 2);
        return BinaryPrimitives.ReadInt16BigEndian(_buffer);
    }

    public int ReadInt32()
    {
        ReadExactly(_buffer, 0, 4);
        return BinaryPrimitives.ReadInt32BigEndian(_buffer);
    }

    public long ReadInt64()
    {
        ReadExactly(_buffer, 0, 8);
        return BinaryPrimitives.ReadInt64BigEndian(_buffer);
    }

    public uint ReadUInt32()
    {
        ReadExactly(_buffer, 0, 4);
        return BinaryPrimitives.ReadUInt32BigEndian(_buffer);
    }

    public string? ReadString()
    {
        var length = ReadInt16();
        if (length == -1) return null;
        if (length == 0) return string.Empty;

        var bytes = new byte[length];
        ReadExactly(bytes, 0, length);
        return Encoding.UTF8.GetString(bytes);
    }

    public byte[] ReadBytes()
    {
        var length = ReadInt32();
        if (length == -1) return Array.Empty<byte>();
        if (length == 0) return Array.Empty<byte>();

        var bytes = new byte[length];
        ReadExactly(bytes, 0, length);
        return bytes;
    }

    public ReadOnlyMemory<byte>? ReadNullableBytes()
    {
        var length = ReadInt32();
        if (length == -1) return null;
        if (length == 0) return ReadOnlyMemory<byte>.Empty;

        var bytes = new byte[length];
        ReadExactly(bytes, 0, length);
        return bytes;
    }

    public bool ReadBool()
    {
        var b = _stream.ReadByte();
        if (b == -1) throw new EndOfStreamException();
        return b != 0;
    }

    public int ReadVarInt()
    {
        var unsigned = ReadUnsignedVarInt();
        // ZigZag decoding
        return (int)(unsigned >> 1) ^ -(int)(unsigned & 1);
    }

    public uint ReadUnsignedVarInt()
    {
        uint result = 0;
        int shift = 0;

        while (true)
        {
            var b = _stream.ReadByte();
            if (b == -1) throw new EndOfStreamException();

            result |= (uint)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;

            shift += 7;
            if (shift >= 32) throw new InvalidDataException("VarInt too long");
        }

        return result;
    }

    public void Skip(int count)
    {
        _stream.Seek(count, SeekOrigin.Current);
    }

    private void ReadExactly(byte[] buffer, int offset, int count)
    {
        var totalRead = 0;
        while (totalRead < count)
        {
            var read = _stream.Read(buffer, offset + totalRead, count - totalRead);
            if (read == 0) throw new EndOfStreamException();
            totalRead += read;
        }
    }

    public void Dispose()
    {
        _stream?.Dispose();
    }
}
