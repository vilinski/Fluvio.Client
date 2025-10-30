using System.IO.Hashing;

namespace Fluvio.Client.Protocol;

/// <summary>
/// CRC32 checksum calculation for Fluvio record batches
/// Uses the CRC32-C (Castagnoli) polynomial which is standard for Kafka/Fluvio
/// </summary>
internal static class Crc32
{
    public static uint Compute(byte[] data)
    {
        return Compute(data, 0, data.Length);
    }

    public static uint Compute(byte[] data, int offset, int length)
    {
        // Use custom CRC-32C implementation instead of System.IO.Hashing
        return Crc32C.Compute(data, offset, length);
    }
}
