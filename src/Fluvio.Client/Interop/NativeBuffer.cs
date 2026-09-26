// src/Fluvio.Client/Interop/NativeBuffer.cs
//
// Reads/frees the `FFIRecord`/`FFIRecordArray` payloads produced by the native consumer FFI
// (see native/fluvio-dotnet/src/ffi_types.rs and consumer.rs). Only the fetch-batch array path
// is needed for Task 4; the single-record `NativeBuffer : SafeHandle` zero-copy path for
// streaming is added in Task 5 alongside `ffi_stream_next`.
using System.Runtime.InteropServices;
using Fluvio.Client.Abstractions;

namespace Fluvio.Client.Interop;

[StructLayout(LayoutKind.Sequential)]
internal readonly struct FFIRecord
{
    public readonly long Offset;
    public readonly long Timestamp;
    public readonly uint Partition;
    public readonly FFISlice Key;
    public readonly FFISlice Value;
}

[StructLayout(LayoutKind.Sequential)]
internal readonly struct FFIRecordArray
{
    public readonly nint Records;
    public readonly nuint Len;
}

internal static class NativeBuffer
{
    internal static unsafe IReadOnlyList<ConsumeRecord> ReadRecordArrayAndFree(nint arrayPtr, int partition)
    {
        if (arrayPtr == 0)
        {
            return Array.Empty<ConsumeRecord>();
        }

        try
        {
            var header = *(FFIRecordArray*)arrayPtr;
            var recordPtrs = (nint*)header.Records;
            var result = new List<ConsumeRecord>((int)header.Len);
            for (nuint i = 0; i < header.Len; i++)
            {
                var record = *(FFIRecord*)recordPtrs[i];
                ReadOnlyMemory<byte>? key = record.Key.Ptr == 0 ? null : record.Key.AsSpan().ToArray();
                var value = record.Value.AsSpan().ToArray();

                result.Add(new ConsumeRecord(
                    Offset: record.Offset,
                    Value: value,
                    Key: key,
                    Timestamp: DateTimeOffset.FromUnixTimeMilliseconds(record.Timestamp),
                    Partition: partition));
            }

            return result;
        }
        finally
        {
            Native.RecordArrayFree(arrayPtr);
        }
    }
}
