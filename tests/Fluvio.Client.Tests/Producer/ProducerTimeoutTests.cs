using Fluvio.Client.Abstractions;
using Fluvio.Client.Interop;
using Fluvio.Client.Producer;

namespace Fluvio.Client.Tests.Producer;

public class ProducerTimeoutTests
{
    [Fact]
    public void EffectiveSendTimeout_UsesProducerOptionsTimeout_NotHardcodedThirtySeconds()
    {
        // Important regression: SendAsync/FlushAsync/DisposeAsync's flush used to always apply the
        // internal 30s FluvioProducer.SendTimeout constant via CreateSendTimeoutSource(token), never
        // reading ProducerOptions.Timeout (documented default: 5s) at all - so no value a caller set
        // here had any effect on send/flush timeout behavior. A fake handle proves this without a
        // real cluster: RustResource's drop callback is never invoked by anything this test does.
        using var fakeHandle = new RustResource((nint)1, _ => { });
        var producer = new FluvioProducer(fakeHandle, new ProducerOptions(Timeout: TimeSpan.FromSeconds(2)));

        Assert.Equal(TimeSpan.FromSeconds(2), producer.EffectiveSendTimeout);
        Assert.NotEqual(FluvioProducer.SendTimeout, producer.EffectiveSendTimeout);
    }


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
