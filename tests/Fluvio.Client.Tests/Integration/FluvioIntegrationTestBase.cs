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
        // Connect to local Fluvio SPU and SC
        var options = new FluvioClientOptions(
            SpuEndpoint: "localhost:9010",  // SPU endpoint
            ScEndpoint: "localhost:9003",   // SC endpoint
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

        // Retry logic for cluster under load
        var maxRetries = 3;
        for (var attempt = 0; attempt < maxRetries; attempt++)
        {
            try
            {
                await admin.CreateTopicAsync(topicName, new TopicSpec(
                    Partitions: partitions,
                    ReplicationFactor: 1
                ));

                // Success - topic created
                return topicName;
            }
            catch (FluvioException ex) when (ex.Message.Contains("already exists"))
            {
                // Topic already exists (from timeout retry), that's okay
                return topicName;
            }
            catch (TaskCanceledException) when (attempt < maxRetries - 1)
            {
                // Cluster timeout - topic might have been created, check first
                await Task.Delay(500);

                try
                {
                    var topics = await admin.ListTopicsAsync();
                    if (topics.Any(t => t.Name == topicName))
                    {
                        // Topic exists - timeout happened after creation
                        return topicName;
                    }
                }
                catch
                {
                    // Ignore list error, will retry create
                }

                // Wait before retry with exponential backoff
                await Task.Delay(1000 * (attempt + 1));
            }
        }

        // Last attempt - catch AlreadyExists error
        try
        {
            await admin.CreateTopicAsync(topicName, new TopicSpec(
                Partitions: partitions,
                ReplicationFactor: 1
            ));
        }
        catch (FluvioException ex) when (ex.Message.Contains("already exists"))
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
