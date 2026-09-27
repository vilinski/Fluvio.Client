using System.Diagnostics;
using System.Globalization;
using System.Text;
using Fluvio.Client;
using Fluvio.Client.Abstractions;

// Soak test: runs producer and consumer loops concurrently for a configurable duration,
// deliberately churning native handle creation/disposal and injecting cancellations and
// induced errors (the FFI/native-handle paths most likely to leak), while periodically
// sampling process memory to a CSV so growth over time can be inspected afterwards.
//
// Usage: dotnet run -c Release -- [--duration-hours 3] [--report-interval-seconds 30] [--topic soak-test]
//
// FLUVIO_TEST_PROFILE selects a named Fluvio profile (e.g. "hetzner-tls"), matching
// tests/Fluvio.Client.Tests/Integration/IntegrationTestConfig.cs; unset connects to a local
// cluster with no TLS.

var duration = TimeSpan.FromHours(3);
var reportInterval = TimeSpan.FromSeconds(30);
var topic = "soak-test";

for (var i = 0; i < args.Length - 1; i++)
{
    switch (args[i])
    {
        case "--duration-hours":
            duration = TimeSpan.FromHours(double.Parse(args[++i], CultureInfo.InvariantCulture));
            break;
        case "--report-interval-seconds":
            reportInterval = TimeSpan.FromSeconds(double.Parse(args[++i], CultureInfo.InvariantCulture));
            break;
        case "--topic":
            topic = args[++i];
            break;
    }
}

var profile = Environment.GetEnvironmentVariable("FLUVIO_TEST_PROFILE");
var clientOptions = string.IsNullOrWhiteSpace(profile)
    ? new FluvioClientOptions(ScEndpoint: "localhost:9003", UseTls: false, ClientId: "soak-test")
    : new FluvioClientOptions(Profile: profile, ClientId: "soak-test");

var stats = new SoakStats();
var deadline = DateTimeOffset.UtcNow + duration;
var csvPath = Path.Combine(AppContext.BaseDirectory, $"soak-report-{DateTime.UtcNow:yyyyMMdd-HHmmss}.csv");

Console.WriteLine($"Soak test starting: duration={duration}, reportInterval={reportInterval}, topic={topic}");
Console.WriteLine($"CSV report: {csvPath}");

await using var client = await FluvioClient.ConnectAsync(clientOptions);
var admin = client.Admin();
try
{
    await admin.CreateTopicAsync(topic, new TopicSpec(Partitions: 1, ReplicationFactor: 1));
}
catch
{
    // Already exists from a previous run - fine, the soak test reuses it.
}

using var cts = new CancellationTokenSource(duration);
var token = cts.Token;

var producerTask = ProducerLoopAsync(client, topic, stats, token);
var consumerTask = ConsumerLoopAsync(client, topic, stats, token);
var reporterTask = ReporterLoopAsync(stats, reportInterval, deadline, csvPath, token);

await Task.WhenAll(
    producerTask.ContinueWith(t => LogIfFaulted("producer loop", t), TaskScheduler.Default),
    consumerTask.ContinueWith(t => LogIfFaulted("consumer loop", t), TaskScheduler.Default),
    reporterTask);

Console.WriteLine();
Console.WriteLine("=== Soak test finished ===");
stats.PrintSummary();

static void LogIfFaulted(string name, Task t)
{
    if (t.IsFaulted)
    {
        Console.WriteLine($"[{name}] terminated with unexpected exception: {t.Exception}");
    }
}

// Producer loop: mostly reuses one producer for throughput, but periodically disposes and
// recreates it to exercise handle-creation/teardown churn. A fraction of sends are cancelled
// via an already-very-short-fused token (real mid-flight cancellation, not pre-cancelled), and
// a fraction target a nonexistent topic to exercise the native error path.
static async Task ProducerLoopAsync(FluvioClient client, string topic, SoakStats stats, CancellationToken token)
{
    var rng = new Random();
    var producer = client.Producer();
    var sendsSinceRecreate = 0;

    while (!token.IsCancellationRequested)
    {
        try
        {
            if (sendsSinceRecreate >= 500)
            {
                await producer.DisposeAsync();
                producer = client.Producer();
                sendsSinceRecreate = 0;
                stats.IncrementProducerRecreated();
            }

            var payload = Encoding.UTF8.GetBytes($"soak-{DateTime.UtcNow:O}-{rng.Next()}");
            var roll = rng.Next(100);

            if (roll < 5)
            {
                // Real mid-flight cancellation: a live token that fires almost immediately,
                // racing the actual native send rather than being pre-cancelled.
                using var shortCts = CancellationTokenSource.CreateLinkedTokenSource(token);
                shortCts.CancelAfter(TimeSpan.FromMilliseconds(1));
                try
                {
                    await producer.SendAsync(topic, payload, cancellationToken: shortCts.Token);
                    stats.IncrementSent();
                }
                catch (OperationCanceledException)
                {
                    stats.IncrementCancelled();
                }
            }
            else if (roll < 8)
            {
                // Induced error path: nonexistent topic.
                try
                {
                    await producer.SendAsync("soak-test-nonexistent-topic", payload, cancellationToken: token);
                }
                catch (FluvioException)
                {
                    stats.IncrementInducedError();
                }
            }
            else
            {
                await producer.SendAsync(topic, payload, cancellationToken: token);
                stats.IncrementSent();
            }

            sendsSinceRecreate++;
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            break;
        }
        catch (Exception ex)
        {
            stats.IncrementUnexpectedError();
            Console.WriteLine($"[producer] unexpected error: {ex.GetType().Name}: {ex.Message}");
            await Task.Delay(TimeSpan.FromMilliseconds(100), token).ContinueWith(_ => { }, TaskScheduler.Default);
        }
    }

    await producer.DisposeAsync();
}

// Consumer loop: periodically recreates the consumer, streams with a randomized short-lived
// cancellation (genuine mid-stream cancellation, exercising StreamAsync's dispose/cleanup path
// repeatedly), and occasionally fetches an invalid partition to exercise the error path.
static async Task ConsumerLoopAsync(FluvioClient client, string topic, SoakStats stats, CancellationToken token)
{
    var rng = new Random();

    while (!token.IsCancellationRequested)
    {
        var consumer = client.Consumer();
        try
        {
            var roll = rng.Next(100);
            if (roll < 10)
            {
                try
                {
                    await consumer.FetchBatchAsync(topic, partition: 999, offset: 0, cancellationToken: token);
                }
                catch (FluvioException)
                {
                    stats.IncrementInducedError();
                }
            }
            else
            {
                using var streamCts = CancellationTokenSource.CreateLinkedTokenSource(token);
                streamCts.CancelAfter(TimeSpan.FromMilliseconds(rng.Next(50, 500)));
                try
                {
                    await foreach (var record in consumer.StreamAsync(topic, offset: 0, cancellationToken: streamCts.Token))
                    {
                        stats.IncrementConsumed();
                        _ = record.Value.Length;
                    }
                }
                catch (OperationCanceledException)
                {
                    stats.IncrementCancelled();
                }
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            break;
        }
        catch (Exception ex)
        {
            stats.IncrementUnexpectedError();
            Console.WriteLine($"[consumer] unexpected error: {ex.GetType().Name}: {ex.Message}");
        }
        finally
        {
            await consumer.DisposeAsync();
            stats.IncrementConsumerRecreated();
        }
    }
}

static async Task ReporterLoopAsync(SoakStats stats, TimeSpan interval, DateTimeOffset deadline, string csvPath, CancellationToken token)
{
    await using var writer = new StreamWriter(csvPath, append: false);
    await writer.WriteLineAsync("timestamp,elapsed_s,working_set_mb,gc_total_mb,gen0,gen1,gen2,sent,consumed,cancelled,induced_errors,unexpected_errors,producer_recreated,consumer_recreated");

    var start = DateTimeOffset.UtcNow;
    while (DateTimeOffset.UtcNow < deadline)
    {
        try
        {
            await Task.Delay(interval, token);
        }
        catch (OperationCanceledException)
        {
            break;
        }

        var workingSetMb = Environment.WorkingSet / (1024.0 * 1024.0);
        var gcTotalMb = GC.GetTotalMemory(forceFullCollection: false) / (1024.0 * 1024.0);
        var elapsed = DateTimeOffset.UtcNow - start;

        var line = string.Create(CultureInfo.InvariantCulture, $"{DateTimeOffset.UtcNow:O},{elapsed.TotalSeconds:F0},{workingSetMb:F1},{gcTotalMb:F1}," +
            $"{GC.CollectionCount(0)},{GC.CollectionCount(1)},{GC.CollectionCount(2)}," +
            $"{stats.Sent},{stats.Consumed},{stats.Cancelled},{stats.InducedErrors},{stats.UnexpectedErrors},{stats.ProducerRecreated},{stats.ConsumerRecreated}");
        await writer.WriteLineAsync(line);
        await writer.FlushAsync();

        Console.WriteLine($"[{elapsed:hh\\:mm\\:ss}] workingSet={workingSetMb:F1}MB gcHeap={gcTotalMb:F1}MB " +
            $"sent={stats.Sent} consumed={stats.Consumed} cancelled={stats.Cancelled} " +
            $"inducedErrors={stats.InducedErrors} unexpectedErrors={stats.UnexpectedErrors} " +
            $"producerRecreated={stats.ProducerRecreated} consumerRecreated={stats.ConsumerRecreated}");
    }
}

sealed class SoakStats
{
    private long _sent;
    private long _consumed;
    private long _cancelled;
    private long _inducedErrors;
    private long _unexpectedErrors;
    private long _producerRecreated;
    private long _consumerRecreated;
    private readonly Stopwatch _stopwatch = Stopwatch.StartNew();

    public long Sent => Interlocked.Read(ref _sent);
    public long Consumed => Interlocked.Read(ref _consumed);
    public long Cancelled => Interlocked.Read(ref _cancelled);
    public long InducedErrors => Interlocked.Read(ref _inducedErrors);
    public long UnexpectedErrors => Interlocked.Read(ref _unexpectedErrors);
    public long ProducerRecreated => Interlocked.Read(ref _producerRecreated);
    public long ConsumerRecreated => Interlocked.Read(ref _consumerRecreated);

    public void IncrementSent() => Interlocked.Increment(ref _sent);
    public void IncrementConsumed() => Interlocked.Increment(ref _consumed);
    public void IncrementCancelled() => Interlocked.Increment(ref _cancelled);
    public void IncrementInducedError() => Interlocked.Increment(ref _inducedErrors);
    public void IncrementUnexpectedError() => Interlocked.Increment(ref _unexpectedErrors);
    public void IncrementProducerRecreated() => Interlocked.Increment(ref _producerRecreated);
    public void IncrementConsumerRecreated() => Interlocked.Increment(ref _consumerRecreated);

    public void PrintSummary()
    {
        Console.WriteLine($"Elapsed: {_stopwatch.Elapsed}");
        Console.WriteLine($"Sent: {Sent}, Consumed: {Consumed}, Cancelled: {Cancelled}");
        Console.WriteLine($"Induced errors (expected): {InducedErrors}");
        Console.WriteLine($"Unexpected errors: {UnexpectedErrors}" + (UnexpectedErrors > 0 ? " <-- investigate" : ""));
        Console.WriteLine($"Producer handles recreated: {ProducerRecreated}, Consumer handles recreated: {ConsumerRecreated}");
        Console.WriteLine("Inspect the CSV's working_set_mb/gc_total_mb columns for growth over time: a real native-handle");
        Console.WriteLine("leak shows as working_set_mb climbing roughly linearly with elapsed time/recreate counts and never");
        Console.WriteLine("plateauing after GC, rather than settling into a steady-state range.");
    }
}
