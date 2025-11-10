using System.Text;
using Fluvio.Client;
using Fluvio.Client.Abstractions;
using Microsoft.Extensions.Logging;

// Create console logger to see logging in action
var loggerFactory = LoggerFactory.Create(builder =>
{
    builder
        .SetMinimumLevel(LogLevel.Debug)
        .AddConsole()
        .AddFilter("Fluvio", LogLevel.Debug);
});

Console.WriteLine("=== Fluvio Client Logging Example ===\n");

// Configure client with logging and retry
var options = new FluvioClientOptions(
    SpuEndpoint: "localhost:9010",
    ScEndpoint: "localhost:9003",
    LoggerFactory: loggerFactory,
    MaxRetries: 3,
    RetryBaseDelay: TimeSpan.FromMilliseconds(100)
);

try
{
    Console.WriteLine("Connecting to Fluvio cluster...\n");
    var client = await FluvioClient.ConnectAsync(options);

    Console.WriteLine("\nClient connected successfully! Check logs above for details.\n");

    // Create producer - will log creation
    var producer = client.Producer();

    Console.WriteLine("Disposing client...\n");
    await client.DisposeAsync();

    Console.WriteLine("\nExample completed successfully!");
}
catch (Exception ex)
{
    Console.WriteLine($"\nError: {ex.Message}");
    Console.WriteLine("Check logs above for detailed error information.");
}
