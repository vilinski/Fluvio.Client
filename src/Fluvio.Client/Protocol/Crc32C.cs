namespace Fluvio.Client.Protocol;

/// <summary>
/// CRC-32C (Castagnoli) implementation to match Rust crc32c crate
/// Polynomial: 0x1EDC6F41 (Castagnoli)
/// </summary>
internal static class Crc32C
{
    // Precomputed lookup table for CRC-32C (Castagnoli polynomial)
    private static readonly uint[] Table = GenerateTable();

    private static uint[] GenerateTable()
    {
        const uint polynomial = 0x82F63B78; // Reversed Castagnoli polynomial
        var table = new uint[256];

        for (uint i = 0; i < 256; i++)
        {
            var crc = i;
            for (var j = 0; j < 8; j++)
            {
                if ((crc & 1) == 1)
                    crc = (crc >> 1) ^ polynomial;
                else
                    crc >>= 1;
            }
            table[i] = crc;
        }

        return table;
    }

    public static uint Compute(byte[] data)
    {
        return Compute(data, 0, data.Length);
    }

    public static uint Compute(byte[] data, int offset, int length)
    {
        var crc = 0xFFFFFFFF; // Initial value

        for (var i = offset; i < offset + length; i++)
        {
            var index = (byte)((crc ^ data[i]) & 0xFF);
            crc = (crc >> 8) ^ Table[index];
        }

        return ~crc; // Final XOR
    }
}
