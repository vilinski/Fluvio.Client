// src/Fluvio.Client/Interop/NativeBuffer.cs
//
// Reads/frees the `FFIRecord`/`FFIRecordArray` payloads produced by the native consumer FFI
// (see native/fluvio-dotnet/src/ffi_types.rs and consumer.rs).
using System.Runtime.InteropServices;
using Fluvio.Client.Abstractions;
using Microsoft.Win32.SafeHandles;

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

/// <summary>
/// Wraps a single, individually-boxed native <c>FFIRecord*</c> (as returned by the streaming
/// <c>ffi_stream_next</c> FFI call) and frees it via <c>ffi_record_free</c> on dispose.
/// </summary>
/// <remarks>
/// This takes the simpler eager-copy path rather than a zero-copy <c>MemoryManager&lt;byte&gt;</c>
/// over the native buffer: a <c>MemoryManager</c> handed out from here would dangle the moment this
/// handle is disposed (which happens as soon as the key/value bytes have been copied out), so
/// <see cref="ToConsumeRecord(nint,int)"/> copies them into managed arrays via <c>ToArray()</c>
/// instead — correctness over zero-copy for this task, per the plan's fallback note (spec §6).
/// </remarks>
internal sealed class NativeBuffer : SafeHandleZeroOrMinusOneIsInvalid
{
    public NativeBuffer(nint handle) : base(ownsHandle: true) => SetHandle(handle);

    protected override bool ReleaseHandle()
    {
        Native.RecordFree(handle);
        return true;
    }

    /// <summary>
    /// Reads and frees a single, individually-boxed <c>FFIRecord*</c> (from <c>ffi_stream_next</c>),
    /// copying its key/value into managed arrays.
    /// </summary>
    internal static unsafe ConsumeRecord ToConsumeRecord(nint recordPtr, int partitionOverride)
    {
        using var buffer = new NativeBuffer(recordPtr);
        var record = *(FFIRecord*)buffer.handle;
        return ToConsumeRecord(record, partitionOverride);
    }

    /// <summary>
    /// Reads the <c>FFIRecordArray</c> header produced by <c>ffi_consumer_fetch_batch</c>, copies
    /// every element's key/value into managed arrays, and frees the whole array (including every
    /// element it owns) in a single <c>ffi_record_array_free</c> call.
    /// </summary>
    /// <remarks>
    /// Each element pointer is read in place, never wrapped in its own <see cref="NativeBuffer"/>:
    /// <c>ffi_record_array_free</c> already frees every <c>FFIRecord*</c> it owns as part of freeing
    /// the array, so individually freeing an element first (the way <see cref="ToConsumeRecord(nint,int)"/>
    /// frees a standalone streaming record) would double-free it.
    /// </remarks>
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
                result.Add(ToConsumeRecord(record, partition));
            }

            return result;
        }
        finally
        {
            Native.RecordArrayFree(arrayPtr);
        }
    }

    private static ConsumeRecord ToConsumeRecord(FFIRecord record, int partitionOverride)
    {
        ReadOnlyMemory<byte>? key = record.Key.Ptr == 0 ? null : record.Key.AsSpan().ToArray();
        var value = record.Value.AsSpan().ToArray();

        return new ConsumeRecord(
            Offset: record.Offset,
            Value: value,
            Key: key,
            Timestamp: DateTimeOffset.FromUnixTimeMilliseconds(record.Timestamp),
            Partition: partitionOverride);
    }
}
