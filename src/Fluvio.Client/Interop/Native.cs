// src/Fluvio.Client/Interop/Native.cs
using System.Runtime.InteropServices;

namespace Fluvio.Client.Interop;

internal static partial class Native
{
    private const string LibraryName = "fluvio_dotnet";

    static Native()
    {
        NativeLibrary.SetDllImportResolver(typeof(Native).Assembly, Resolve);
        RuntimeInit();
    }

    private static nint Resolve(string libraryName, System.Reflection.Assembly assembly, DllImportSearchPath? searchPath)
    {
        if (libraryName != LibraryName) return 0;

        var envPath = Environment.GetEnvironmentVariable("FLUVIO_DOTNET_NATIVE_PATH");
        if (!string.IsNullOrEmpty(envPath) && NativeLibrary.TryLoad(envPath, out var envHandle))
            return envHandle;

        foreach (var config in new[] { "debug", "release" })
        {
            // native/fluvio-dotnet is a standalone Cargo crate (not a workspace member), so
            // its build output lands in native/fluvio-dotnet/target/, not native/target/.
            var repoPath = Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..",
                "native", "fluvio-dotnet", "target", config, MapLibraryFileName(libraryName));
            if (File.Exists(repoPath) && NativeLibrary.TryLoad(repoPath, out var repoHandle))
                return repoHandle;
        }

        if (NativeLibrary.TryLoad(libraryName, assembly, searchPath, out var defaultHandle))
            return defaultHandle;

        throw new DllNotFoundException(
            $"Could not locate native library '{libraryName}'. Set FLUVIO_DOTNET_NATIVE_PATH, " +
            "run 'cargo build' in native/fluvio-dotnet, or ensure the NuGet package's runtimes/ " +
            "assets are present.");
    }

    private static string MapLibraryFileName(string name) =>
        OperatingSystem.IsWindows() ? $"{name}.dll" :
        OperatingSystem.IsMacOS() ? $"lib{name}.dylib" : $"lib{name}.so";

    [LibraryImport(LibraryName, EntryPoint = "ffi_runtime_init")]
    private static partial int RuntimeInitNative();

    private static void RuntimeInit() => RuntimeInitNative();

    [LibraryImport(LibraryName, EntryPoint = "ffi_client_connect")]
    internal static unsafe partial void ClientConnect(byte* configJson, nuint configJsonLen, Tcb tcb);

    [LibraryImport(LibraryName, EntryPoint = "ffi_client_health_check")]
    internal static partial void ClientHealthCheck(nint client, Tcb tcb);

    [LibraryImport(LibraryName, EntryPoint = "ffi_client_drop")]
    internal static partial void ClientDrop(nint client);

    [LibraryImport(LibraryName, EntryPoint = "ffi_string_free")]
    internal static partial void StringFree(nint ptr);

    [LibraryImport(LibraryName, EntryPoint = "ffi_producer_new")]
    internal static unsafe partial void ProducerNew(nint client, byte* topic, nuint topicLen, Tcb tcb);

    [LibraryImport(LibraryName, EntryPoint = "ffi_producer_send")]
    internal static unsafe partial void ProducerSend(nint producer, byte* key, nuint keyLen, byte* value, nuint valueLen, Tcb tcb);

    [LibraryImport(LibraryName, EntryPoint = "ffi_producer_flush")]
    internal static partial void ProducerFlush(nint producer, Tcb tcb);

    [LibraryImport(LibraryName, EntryPoint = "ffi_producer_drop")]
    internal static partial void ProducerDrop(nint producer);

    internal static unsafe string? ReadAndFreeString(nint ptr)
    {
        if (ptr == 0) return null;
        var s = Marshal.PtrToStringUTF8(ptr);
        StringFree(ptr);
        return s;
    }
}
