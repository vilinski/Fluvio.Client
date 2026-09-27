// src/Fluvio.Client/Interop/Callbacks.cs
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Fluvio.Client.Interop;

internal static class Callbacks
{
    private static readonly unsafe nint OnSuccessPtr =
        (nint)(delegate* unmanaged[Cdecl]<nint, nint, void>)&OnSuccess;
    private static readonly unsafe nint OnFailurePtr =
        (nint)(delegate* unmanaged[Cdecl]<nint, int, byte*, nuint, void>)&OnFailure;

    internal static Task<nint> CallAsync(Action<Tcb> invoke)
    {
        var tcs = new TaskCompletionSource<nint>(TaskCreationOptions.RunContinuationsAsynchronously);
        var gch = GCHandle.Alloc(tcs);
        try
        {
            invoke(new Tcb
            {
                Tcs = GCHandle.ToIntPtr(gch),
                OnSuccess = OnSuccessPtr,
                OnFailure = OnFailurePtr,
            });
        }
        catch
        {
            gch.Free();
            throw;
        }
        return tcs.Task;
    }

    [UnmanagedCallersOnly(CallConvs = new[] { typeof(CallConvCdecl) })]
    private static void OnSuccess(nint tcsHandle, nint result)
    {
        var gch = GCHandle.FromIntPtr(tcsHandle);
        var tcs = (TaskCompletionSource<nint>)gch.Target!;
        gch.Free();
        tcs.SetResult(result);
    }

    [UnmanagedCallersOnly(CallConvs = new[] { typeof(CallConvCdecl) })]
    private static unsafe void OnFailure(nint tcsHandle, int code, byte* message, nuint len)
    {
        var gch = GCHandle.FromIntPtr(tcsHandle);
        var tcs = (TaskCompletionSource<nint>)gch.Target!;
        gch.Free();
        string? msg = message != null && len > 0
            ? Encoding.UTF8.GetString(new ReadOnlySpan<byte>(message, checked((int)len)))
            : null;
        tcs.SetException(FluvioException.FromCode(code, msg));
    }
}
