// src/Fluvio.Client/Interop/NativeTypes.cs
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Fluvio.Client.Interop;

[StructLayout(LayoutKind.Sequential)]
internal readonly unsafe struct FFISlice
{
    public readonly nint Ptr;
    public readonly nuint Len;

    public ReadOnlySpan<byte> AsSpan() =>
        Ptr == 0 || Len == 0 ? ReadOnlySpan<byte>.Empty : new ReadOnlySpan<byte>((void*)Ptr, checked((int)Len));
}

[StructLayout(LayoutKind.Sequential)]
internal struct Tcb
{
    public nint Tcs;
    public nint OnSuccess;
    public nint OnFailure;
}

internal static class NativeTypeAsserts
{
    static unsafe NativeTypeAsserts()
    {
        Debug.Assert(sizeof(FFISlice) == 16, "FFISlice size mismatch with Rust ffi_types::FFISlice");
        Debug.Assert(Marshal.SizeOf<Tcb>() == 24, "Tcb size mismatch with Rust tcb::Tcb");
    }
}
