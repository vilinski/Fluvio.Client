using Fluvio.Client.Producer;

namespace Fluvio.Client.Tests.Producer;

public class ProducerTimeoutTests
{
    [Fact]
    public void CreateSendTimeoutSource_PropagatesCallerCancellation()
    {
        using var callerCts = new CancellationTokenSource();
        using var linked = FluvioProducer.CreateSendTimeoutSource(callerCts.Token);

        Assert.False(linked.IsCancellationRequested);

        callerCts.Cancel();

        Assert.True(linked.IsCancellationRequested);
    }

    [Fact]
    public void CreateSendTimeoutSource_IsLinkedNotFresh_CancellingUnrelatedTokenDoesNotAffectIt()
    {
        using var callerCts = new CancellationTokenSource();
        using var unrelatedCts = new CancellationTokenSource();
        using var linked = FluvioProducer.CreateSendTimeoutSource(callerCts.Token);

        unrelatedCts.Cancel();

        Assert.False(linked.IsCancellationRequested);

        callerCts.Cancel();

        Assert.True(linked.IsCancellationRequested);
    }
}
