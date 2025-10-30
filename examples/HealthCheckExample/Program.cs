using System.Text;
using Fluvio.Client;
using Fluvio.Client.Abstractions;
using Microsoft.Extensions.Logging;

var loggerFactory = LoggerFactory.Create(builder =>
{
    builder
        .SetMinimumLevel(LogLevel.Information)
        .AddConsole();
});

Console.WriteLine("=== Fluvio Health Check Example ===\n");

var options = new FluvioClientOptions(
    Endpoint: "localhost:9010",
    ScEndpoint: "localhost:9003",
    LoggerFactory: loggerFactory
);

try
{
    Console.WriteLine("Connecting to Fluvio cluster...");
    var client = await FluvioClient.ConnectAsync(options);
    Console.WriteLine("✓ Connected\n");

    // Perform health check
    Console.WriteLine("Performing health check...");
    var health = await client.CheckHealthAsync();

    Console.WriteLine($"\nHealth Check Results:");
    Console.WriteLine($"  Overall Status: {(health.IsHealthy ? "✓ HEALTHY" : "✗ UNHEALTHY")}");
    Console.WriteLine($"  SPU Connected: {(health.SpuConnected ? "✓" : "✗")}");
    Console.WriteLine($"  SC Connected:  {(health.ScConnected == true ? "✓" : health.ScConnected == false ? "✗" : "N/A")}");

    if (health.LastSuccessfulRequestDuration.HasValue)
    {
        Console.WriteLine($"  Request Time:  {health.LastSuccessfulRequestDuration.Value.TotalMilliseconds:F2}ms");
    }

    if (!string.IsNullOrEmpty(health.ErrorMessage))
    {
        Console.WriteLine($"  Error:         {health.ErrorMessage}");
    }

    Console.WriteLine($"  Timestamp:     {health.CheckTimestamp:yyyy-MM-dd HH:mm:ss} UTC");

    // Continuous monitoring example
    Console.WriteLine("\n--- Continuous Monitoring (5 checks, 2s apart) ---\n");
    for (int i = 1; i <= 5; i++)
    {
        health = await client.CheckHealthAsync();
        var status = health.IsHealthy ? "HEALTHY" : "UNHEALTHY";
        var duration = health.LastSuccessfulRequestDuration?.TotalMilliseconds.ToString("F1") ?? "N/A";
        Console.WriteLine($"[{i}] {health.CheckTimestamp:HH:mm:ss} - {status} - Duration: {duration}ms");

        if (i < 5)
            await Task.Delay(2000);
    }

    await client.DisposeAsync();
    Console.WriteLine("\n✓ Example completed successfully!");
}
catch (Exception ex)
{
    Console.WriteLine($"\n✗ Error: {ex.Message}");
}
