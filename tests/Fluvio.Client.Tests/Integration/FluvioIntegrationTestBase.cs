using Fluvio.Client;
using Fluvio.Client.Abstractions;

namespace Fluvio.Client.Tests.Integration;

/// <summary>
/// Base class for integration tests that require a running Fluvio cluster
/// </summary>
public abstract class FluvioIntegrationTestBase : IAsyncLifetime
{
    protected FluvioClient? Client { get; private set; }

    // Use shorter topic names to stay under 63 character limit
    // Format: test-{8-char-hex} = 13 characters
    protected static string GenerateTopicName() => $"test-{Guid.NewGuid():N}"[..13];

    public async Task InitializeAsync()
    {
        // Connect to local Fluvio SPU (not SC)
        var options = new FluvioClientOptions(
            Endpoint: "localhost:9010",  // SPU endpoint
            UseTls: false,
            ClientId: "integration-test"
        );

        Client = await FluvioClient.ConnectAsync(options);
    }

    public async Task DisposeAsync()
    {
        if (Client != null)
        {
            await Client.DisposeAsync();
        }
    }

    protected async Task<string> CreateTestTopicAsync(int partitions = 1)
    {
        var topicName = GenerateTopicName();
        var admin = Client!.Admin();

        try
        {
            await admin.CreateTopicAsync(topicName, new TopicSpec(
                Partitions: partitions,
                ReplicationFactor: 1
            ));
        }
        catch (FluvioException ex) when (ex.Message.Contains("AlreadyExists"))
        {
            // Topic already exists, that's okay
        }

        return topicName;
    }

    protected async Task CleanupTopicAsync(string topicName)
    {
        try
        {
            var admin = Client!.Admin();
            await admin.DeleteTopicAsync(topicName);
        }
        catch
        {
            // Best effort cleanup
        }
    }
}
