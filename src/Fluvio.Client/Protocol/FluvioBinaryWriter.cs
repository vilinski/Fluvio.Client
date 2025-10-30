using System.Buffers.Binary;
using System.Text;

namespace Fluvio.Client.Protocol;

/// <summary>
/// Binary writer for Fluvio protocol (big-endian, Kafka-inspired)
/// </summary>
internal sealed class FluvioBinaryWriter : IDisposable
{
    internal readonly MemoryStream _stream;
    private readonly byte[] _buffer;

    public FluvioBinaryWriter()
    {
        _stream = new MemoryStream();
        _buffer = new byte[8]; // For primitive type conversions
    }

    public int Position => (int)_stream.Position;

    public void WriteInt8(sbyte value)
    {
        _stream.WriteByte((byte)value);
    }

    public void WriteInt16(short value)
    {
        BinaryPrimitives.WriteInt16BigEndian(_buffer, value);
        _stream.Write(_buffer, 0, 2);
    }

    public void WriteUInt16(ushort value)
    {
        BinaryPrimitives.WriteUInt16BigEndian(_buffer, value);
        _stream.Write(_buffer, 0, 2);
    }

    public void WriteInt32(int value)
    {
        BinaryPrimitives.WriteInt32BigEndian(_buffer, value);
        _stream.Write(_buffer, 0, 4);
    }

    public void WriteInt64(long value)
    {
        BinaryPrimitives.WriteInt64BigEndian(_buffer, value);
        _stream.Write(_buffer, 0, 8);
    }

    public void WriteUInt32(uint value)
    {
        BinaryPrimitives.WriteUInt32BigEndian(_buffer, value);
        _stream.Write(_buffer, 0, 4);
    }

    public void WriteString(string? value)
    {
        if (value == null)
        {
            WriteInt16(-1);
            return;
        }

        var bytes = Encoding.UTF8.GetBytes(value);
        WriteInt16((short)bytes.Length);
        _stream.Write(bytes);
    }

    public void WriteBytes(ReadOnlySpan<byte> value)
    {
        WriteInt32(value.Length);
        _stream.Write(value);
    }

    public void WriteNullableBytes(ReadOnlyMemory<byte>? value)
    {
        if (!value.HasValue || value.Value.Length == 0)
        {
            WriteInt32(-1);
            return;
        }

        WriteInt32(value.Value.Length);
        _stream.Write(value.Value.Span);
    }

    public void WriteBool(bool value)
    {
        _stream.WriteByte(value ? (byte)1 : (byte)0);
    }

    /// <summary>
    /// Write an optional value using Fluvio Option encoding (bool + optional value)
    /// </summary>
    /// <typeparam name="T">The type of value to write</typeparam>
    /// <param name="value">The optional value (null for None)</param>
    /// <param name="writeValue">Function to write the value if present</param>
    public void WriteOption<T>(T? value, Action<T> writeValue) where T : struct
    {
        if (value.HasValue)
        {
            WriteBool(true);  // Some
            writeValue(value.Value);
        }
        else
        {
            WriteBool(false); // None
        }
    }

    /// <summary>
    /// Write an optional reference type using Fluvio Option encoding (bool + optional value)
    /// </summary>
    /// <typeparam name="T">The type of value to write</typeparam>
    /// <param name="value">The optional value (null for None)</param>
    /// <param name="writeValue">Function to write the value if present</param>
    public void WriteOptionRef<T>(T? value, Action<T> writeValue) where T : class
    {
        if (value != null)
        {
            WriteBool(true);  // Some
            writeValue(value);
        }
        else
        {
            WriteBool(false); // None
        }
    }

    /// <summary>
    /// Write an optional string using Fluvio Option encoding (bool + optional string)
    /// </summary>
    public void WriteOptionString(string? value)
    {
        WriteOptionRef(value, WriteString);
    }

    public void WriteVarInt(int value)
    {
        // ZigZag encoding for signed integers
        var unsigned = (uint)((value << 1) ^ (value >> 31));
        WriteUnsignedVarInt(unsigned);
    }

    public void WriteUnsignedVarInt(uint value)
    {
        while ((value & ~0x7F) != 0)
        {
            _stream.WriteByte((byte)((value & 0x7F) | 0x80));
            value >>= 7;
        }
        _stream.WriteByte((byte)value);
    }

    public void WriteVarLong(long value)
    {
        // ZigZag encoding for signed long integers
        var unsigned = (ulong)((value << 1) ^ (value >> 63));
        WriteUnsignedVarLong(unsigned);
    }

    public void WriteUnsignedVarLong(ulong value)
    {
        while ((value & ~0x7FUL) != 0)
        {
            _stream.WriteByte((byte)((value & 0x7F) | 0x80));
            value >>= 7;
        }
        _stream.WriteByte((byte)value);
    }

    public byte[] ToArray() => _stream.ToArray();

    public ReadOnlyMemory<byte> ToMemory()
    {
        if (_stream.TryGetBuffer(out var buffer))
        {
            return buffer.AsMemory();
        }
        return _stream.ToArray();
    }

    public void Dispose()
    {
        _stream?.Dispose();
    }
}
