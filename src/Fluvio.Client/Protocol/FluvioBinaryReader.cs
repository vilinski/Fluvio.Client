using System.Buffers.Binary;
using System.Text;

namespace Fluvio.Client.Protocol;

/// <summary>
/// Binary reader for Fluvio protocol (big-endian, Kafka-inspired)
/// </summary>
internal sealed class FluvioBinaryReader(Stream stream) : IDisposable
{
    private readonly byte[] _buffer = new byte[8];

    public FluvioBinaryReader(ReadOnlyMemory<byte> data)
        : this(new MemoryStream(data.ToArray()))
    {
    }

    public long Position => stream.Position;
    public long Length => stream.Length;
    public bool EndOfStream => stream.Position >= stream.Length;

    public sbyte ReadInt8()
    {
        var b = stream.ReadByte();
        if (b == -1) throw new EndOfStreamException();
        return (sbyte)b;
    }

    public short ReadInt16()
    {
        ReadExactly(_buffer, 0, 2);
        return BinaryPrimitives.ReadInt16BigEndian(_buffer);
    }

    public ushort ReadUInt16()
    {
        ReadExactly(_buffer, 0, 2);
        return BinaryPrimitives.ReadUInt16BigEndian(_buffer);
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

    public ulong ReadUInt64()
    {
        ReadExactly(_buffer, 0, 8);
        return BinaryPrimitives.ReadUInt64BigEndian(_buffer);
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

    /// <summary>
    /// Read an optional value using Fluvio Option encoding (bool + optional value)
    /// </summary>
    /// <typeparam name="T">The type of value to read</typeparam>
    /// <param name="readValue">Function to read the value if present</param>
    /// <returns>The optional value or null if None</returns>
    public T? ReadOption<T>(Func<T> readValue) where T : struct
    {
        var hasValue = ReadBool();
        if (!hasValue) return null;
        return readValue();
    }

    /// <summary>
    /// Read an optional reference type using Fluvio Option encoding (bool + optional value)
    /// </summary>
    /// <typeparam name="T">The type of value to read</typeparam>
    /// <param name="readValue">Function to read the value if present</param>
    /// <returns>The optional value or null if None</returns>
    public T? ReadOptionRef<T>(Func<T> readValue) where T : class
    {
        var hasValue = ReadBool();
        if (!hasValue) return null;
        return readValue();
    }

    /// <summary>
    /// Read optional string (Fluvio Option encoding: bool + value)
    /// </summary>
    public string? ReadOptionalString()
    {
        var hasValue = ReadBool();
        if (!hasValue) return null;
        return ReadString();
    }

    public byte[] ReadBytes()
    {
        var length = ReadInt32();
        if (length == -1) return [];
        if (length == 0) return [];

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
        var b = stream.ReadByte();
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
        var shift = 0;

        while (true)
        {
            var b = stream.ReadByte();
            if (b == -1) throw new EndOfStreamException();

            result |= (uint)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;

            shift += 7;
            if (shift >= 32) throw new InvalidDataException("VarInt too long");
        }

        return result;
    }

    public long ReadVarLong()
    {
        var unsigned = ReadUnsignedVarLong();
        // ZigZag decoding
        return (long)(unsigned >> 1) ^ -(long)(unsigned & 1);
    }

    public ulong ReadUnsignedVarLong()
    {
        ulong result = 0;
        var shift = 0;

        while (true)
        {
            var b = stream.ReadByte();
            if (b == -1) throw new EndOfStreamException();

            result |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;

            shift += 7;
            if (shift >= 64) throw new InvalidDataException("VarLong too long");
        }

        return result;
    }

    /// <summary>
    /// Reads raw bytes without a length prefix
    /// </summary>
    public byte[] ReadRawBytes(int count)
    {
        if (count == 0) return [];

        var bytes = new byte[count];
        ReadExactly(bytes, 0, count);
        return bytes;
    }

    public void Skip(int count)
    {
        stream.Seek(count, SeekOrigin.Current);
    }

    private void ReadExactly(byte[] buffer, int offset, int count)
    {
        var totalRead = 0;
        while (totalRead < count)
        {
            var read = stream.Read(buffer, offset + totalRead, count - totalRead);
            if (read == 0) throw new EndOfStreamException();
            totalRead += read;
        }
    }

    public void Dispose()
    {
        stream?.Dispose();
    }
}
