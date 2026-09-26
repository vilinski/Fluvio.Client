// src/Fluvio.Client/Interop/RustResource.cs
using Microsoft.Win32.SafeHandles;

namespace Fluvio.Client.Interop;

internal sealed class RustResource : SafeHandleZeroOrMinusOneIsInvalid
{
    private readonly Action<nint> _drop;

    public RustResource(nint handle, Action<nint> drop) : base(ownsHandle: true)
    {
        SetHandle(handle);
        _drop = drop;
    }

    protected override bool ReleaseHandle()
    {
        _drop(handle);
        return true;
    }

    public T RunWithIncrement<T>(Func<nint, T> fn)
    {
        bool added = false;
        try
        {
            DangerousAddRef(ref added);
            return fn(DangerousGetHandle());
        }
        finally
        {
            if (added) DangerousRelease();
        }
    }

    public async Task<T> RunAsyncWithIncrement<T>(Func<nint, Task<T>> fn)
    {
        bool added = false;
        try
        {
            DangerousAddRef(ref added);
            return await fn(DangerousGetHandle()).ConfigureAwait(false);
        }
        finally
        {
            if (added) DangerousRelease();
        }
    }
}
