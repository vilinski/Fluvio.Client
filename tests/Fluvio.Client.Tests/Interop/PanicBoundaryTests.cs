using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Fluvio.Client.Interop;

[assembly: DisableRuntimeMarshalling]

namespace Fluvio.Client.Tests.Interop;

public partial class PanicBoundaryTests
{
    static PanicBoundaryTests() =>
        Native.RegisterResolverFor(typeof(PanicBoundaryTests).Assembly);

    [LibraryImport("fluvio_dotnet", EntryPoint = "ffi_debug_trigger_panic")]
    private static partial void DebugTriggerPanic(Tcb tcb);

    [Fact]
    public async Task NativePanicCompletesTaskWithFailureInsteadOfHanging()
    {
        var task = Callbacks.CallAsync(tcb => DebugTriggerPanic(tcb));
        var completed = await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(5)));
        Assert.Same(task, completed);
        await Assert.ThrowsAsync<FluvioException>(() => task);
    }
}
