// src/Fluvio.Client/Interop/CancellationBridge.cs
namespace Fluvio.Client.Interop;

/// <summary>
/// Bridges a C# <see cref="CancellationToken"/> to a native <c>CancelHandle</c>
/// (see <c>native/fluvio-dotnet/src/cancel.rs</c>), so every non-streaming FFI call can be
/// raced against real cancellation on the Rust side instead of only on the C# `Task` wait.
/// </summary>
internal static class CancellationBridge
{
    /// <summary>
    /// Creates a native cancel handle and, if <paramref name="ct"/> can be cancelled, registers
    /// it to trigger that handle. Callers must dispose the returned registration once the native
    /// call has completed, which also frees the native handle.
    /// </summary>
    public static (nint handle, IDisposable registration) Create(CancellationToken ct)
    {
        var handle = Native.CancelNew();
        if (!ct.CanBeCanceled)
            return (handle, new CombinedDisposable(default, handle));

        var reg = ct.Register(static state =>
        {
            var h = (nint)state!;
            Native.CancelTrigger(h);
        }, handle);

        return (handle, new CombinedDisposable(reg, handle));
    }

    private sealed class CombinedDisposable(CancellationTokenRegistration reg, nint handle) : IDisposable
    {
        public void Dispose()
        {
            reg.Dispose();
            Native.CancelDrop(handle);
        }
    }
}
