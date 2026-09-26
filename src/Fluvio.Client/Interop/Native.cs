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

    /// <summary>
    /// Registers <see cref="Resolve"/> as the native-library resolver for another assembly's
    /// P/Invoke declarations (e.g. test-only `LibraryImport`s that live outside this assembly
    /// and would otherwise miss the repo-relative Cargo build output this resolver finds).
    /// </summary>
    internal static void RegisterResolverFor(System.Reflection.Assembly assembly) =>
        NativeLibrary.SetDllImportResolver(assembly, Resolve);

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
    internal static unsafe partial void ClientConnect(byte* configJson, nuint configJsonLen, nint cancel, Tcb tcb);

    [LibraryImport(LibraryName, EntryPoint = "ffi_client_health_check")]
    internal static partial void ClientHealthCheck(nint client, nint cancel, Tcb tcb);

    [LibraryImport(LibraryName, EntryPoint = "ffi_client_drop")]
    internal static partial void ClientDrop(nint client);

    [LibraryImport(LibraryName, EntryPoint = "ffi_string_free")]
    internal static partial void StringFree(nint ptr);

    [LibraryImport(LibraryName, EntryPoint = "ffi_cancel_new")]
    internal static partial nint CancelNew();

    [LibraryImport(LibraryName, EntryPoint = "ffi_cancel_trigger")]
    internal static partial void CancelTrigger(nint handle);

    [LibraryImport(LibraryName, EntryPoint = "ffi_cancel_drop")]
    internal static partial void CancelDrop(nint handle);

    [LibraryImport(LibraryName, EntryPoint = "ffi_producer_new")]
    internal static unsafe partial void ProducerNew(nint client, byte* topic, nuint topicLen, nint cancel, Tcb tcb);

    [LibraryImport(LibraryName, EntryPoint = "ffi_producer_send")]
    internal static unsafe partial void ProducerSend(nint producer, byte* key, nuint keyLen, byte* value, nuint valueLen, nint cancel, Tcb tcb);

    [LibraryImport(LibraryName, EntryPoint = "ffi_producer_flush")]
    internal static partial void ProducerFlush(nint producer, nint cancel, Tcb tcb);

    [LibraryImport(LibraryName, EntryPoint = "ffi_producer_drop")]
    internal static partial void ProducerDrop(nint producer);

    [LibraryImport(LibraryName, EntryPoint = "ffi_consumer_fetch_batch")]
    internal static unsafe partial void ConsumerFetchBatch(nint client, byte* topic, nuint topicLen, uint partition, long offset, uint maxBytes, nint cancel, Tcb tcb);

    [LibraryImport(LibraryName, EntryPoint = "ffi_record_array_free")]
    internal static partial void RecordArrayFree(nint ptr);

    [LibraryImport(LibraryName, EntryPoint = "ffi_consumer_fetch_last_offset")]
    internal static unsafe partial void ConsumerFetchLastOffset(nint client, byte* consumerId, nuint consumerIdLen, byte* topic, nuint topicLen, uint partition, nint cancel, Tcb tcb);

    [LibraryImport(LibraryName, EntryPoint = "ffi_consumer_commit_offset")]
    internal static unsafe partial void ConsumerCommitOffset(nint client, byte* consumerId, nuint consumerIdLen, byte* topic, nuint topicLen, uint partition, long offset, nint cancel, Tcb tcb);

    [LibraryImport(LibraryName, EntryPoint = "ffi_stream_new")]
    internal static unsafe partial void StreamNew(nint client, byte* topic, nuint topicLen, uint partition, long offset, Tcb tcb);

    [LibraryImport(LibraryName, EntryPoint = "ffi_stream_next")]
    internal static partial void StreamNext(nint stream, Tcb tcb);

    [LibraryImport(LibraryName, EntryPoint = "ffi_stream_close")]
    internal static partial void StreamClose(nint stream);

    [LibraryImport(LibraryName, EntryPoint = "ffi_stream_drop")]
    internal static partial void StreamDrop(nint stream);

    [LibraryImport(LibraryName, EntryPoint = "ffi_record_free")]
    internal static partial void RecordFree(nint ptr);

    [LibraryImport(LibraryName, EntryPoint = "ffi_admin_create_topic")]
    internal static unsafe partial void AdminCreateTopic(nint client, byte* name, nuint nameLen, byte* specJson, nuint specJsonLen, nint cancel, Tcb tcb);

    [LibraryImport(LibraryName, EntryPoint = "ffi_admin_delete_topic")]
    internal static unsafe partial void AdminDeleteTopic(nint client, byte* name, nuint nameLen, nint cancel, Tcb tcb);

    [LibraryImport(LibraryName, EntryPoint = "ffi_admin_list_topics")]
    internal static partial void AdminListTopics(nint client, nint cancel, Tcb tcb);

    [LibraryImport(LibraryName, EntryPoint = "ffi_admin_get_topic")]
    internal static unsafe partial void AdminGetTopic(nint client, byte* name, nuint nameLen, nint cancel, Tcb tcb);

    [LibraryImport(LibraryName, EntryPoint = "ffi_admin_list_spus")]
    internal static partial void AdminListSpus(nint client, nint cancel, Tcb tcb);

    [LibraryImport(LibraryName, EntryPoint = "ffi_admin_get_spu")]
    internal static partial void AdminGetSpu(nint client, int spuId, nint cancel, Tcb tcb);

    [LibraryImport(LibraryName, EntryPoint = "ffi_admin_list_partitions")]
    internal static unsafe partial void AdminListPartitions(nint client, byte* topicFilter, nuint topicFilterLen, nint cancel, Tcb tcb);

    [LibraryImport(LibraryName, EntryPoint = "ffi_admin_get_partition")]
    internal static unsafe partial void AdminGetPartition(nint client, byte* topic, nuint topicLen, uint partition, nint cancel, Tcb tcb);

    [LibraryImport(LibraryName, EntryPoint = "ffi_admin_list_smartmodules")]
    internal static partial void AdminListSmartModules(nint client, nint cancel, Tcb tcb);

    [LibraryImport(LibraryName, EntryPoint = "ffi_admin_get_smartmodule")]
    internal static unsafe partial void AdminGetSmartModule(nint client, byte* name, nuint nameLen, nint cancel, Tcb tcb);

    [LibraryImport(LibraryName, EntryPoint = "ffi_admin_create_smartmodule")]
    internal static unsafe partial void AdminCreateSmartModule(nint client, byte* name, nuint nameLen, byte* wasm, nuint wasmLen, nint cancel, Tcb tcb);

    [LibraryImport(LibraryName, EntryPoint = "ffi_admin_delete_smartmodule")]
    internal static unsafe partial void AdminDeleteSmartModule(nint client, byte* name, nuint nameLen, nint cancel, Tcb tcb);

    internal static unsafe string? ReadAndFreeString(nint ptr)
    {
        if (ptr == 0) return null;
        var s = Marshal.PtrToStringUTF8(ptr);
        StringFree(ptr);
        return s;
    }
}
