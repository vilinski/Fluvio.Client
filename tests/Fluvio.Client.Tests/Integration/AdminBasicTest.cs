using Fluvio.Client.Abstractions;
using Xunit.Abstractions;

namespace Fluvio.Client.Tests.Integration;

/// <summary>
/// Basic Admin API test - Create and Delete topic using SC connection
/// </summary>
public class AdminBasicTest(ITestOutputHelper output)
{
    [Fact]
    public async Task CanCreateAndDeleteTopicViaSC()
    {
        // Connect to Fluvio with SC endpoint for Admin operations
        var options = new FluvioClientOptions(
            Endpoint: "localhost:9010",      // SPU for data operations
            ScEndpoint: "localhost:9003",    // SC for Admin operations
            UseTls: false,
            ClientId: "admin-basic-test"
        );

        await using var client = await FluvioClient.ConnectAsync(options);
        output.WriteLine("Connected to Fluvio SPU!");

        // Get Admin instance (will connect to SC)
        var admin = client.Admin();
        output.WriteLine("Admin instance created (connecting to SC)");

        // Use short topic name (max 63 chars): test-{8 hex chars} = 13 chars
        var topicName = $"test-{Guid.NewGuid():N}"[..13];
        output.WriteLine($"Creating topic: {topicName}");

        try
        {
            // Create topic
            var spec = new TopicSpec(
                Partitions: 1,
                ReplicationFactor: 1,
                IgnoreRackAssignment: false,
                CustomSettings: null
            );

            await admin.CreateTopicAsync(topicName, spec);
            output.WriteLine($"✓ Topic created: {topicName}");

            // Wait a bit for topic to be ready
            await Task.Delay(1000);

            // Delete topic
            output.WriteLine($"Deleting topic: {topicName}");
            await admin.DeleteTopicAsync(topicName);
            output.WriteLine($"✓ Topic deleted: {topicName}");

            // SUCCESS!
            Assert.True(true, "Create and Delete topic succeeded!");
        }
        catch (Exception ex)
        {
            output.WriteLine($"ERROR: {ex.Message}");
            output.WriteLine($"Stack: {ex.StackTrace}");
            throw;
        }
    }

    [Fact]
    public async Task CanListTopicsViaSC()
    {
        // Connect to Fluvio with SC endpoint for Admin operations
        var options = new FluvioClientOptions(
            Endpoint: "localhost:9010",      // SPU for data operations
            ScEndpoint: "localhost:9003",    // SC for Admin operations
            UseTls: false,
            ClientId: "admin-list-test"
        );

        await using var client = await FluvioClient.ConnectAsync(options);
        output.WriteLine("Connected to Fluvio!");

        var admin = client.Admin();
        // Use short topic name (max 63 chars): test-{8 hex chars} = 13 chars
        var topicName = $"test-{Guid.NewGuid():N}"[..13];

        try
        {
            // Create a test topic first
            output.WriteLine($"Creating test topic: {topicName}");
            var spec = new TopicSpec(
                Partitions: 2,
                ReplicationFactor: 1,
                IgnoreRackAssignment: false,
                CustomSettings: null
            );
            await admin.CreateTopicAsync(topicName, spec);
            output.WriteLine($"✓ Topic created");

            // Wait for topic to be available
            await Task.Delay(1000);

            // List all topics
            output.WriteLine("Listing all topics...");
            var topics = await admin.ListTopicsAsync();
            output.WriteLine($"Found {topics.Count} topics:");

            foreach (var topic in topics)
            {
                output.WriteLine($"  - {topic.Name} (partitions: {topic.Partitions}, replication: {topic.ReplicationFactor})");
            }

            // Verify our topic is in the list
            var ourTopic = topics.FirstOrDefault(t => t.Name == topicName);
            Assert.NotNull(ourTopic);
            Assert.Equal(topicName, ourTopic.Name);
            Assert.Equal(2, ourTopic.Partitions);
            Assert.Equal(1, ourTopic.ReplicationFactor);

            output.WriteLine($"✓ Found our topic in the list!");

            // Cleanup
            output.WriteLine($"Deleting test topic: {topicName}");
            await admin.DeleteTopicAsync(topicName);
            output.WriteLine($"✓ Topic deleted");
        }
        catch (Exception ex)
        {
            output.WriteLine($"ERROR: {ex.Message}");
            output.WriteLine($"Stack: {ex.StackTrace}");

            // Try to cleanup
            try { await admin.DeleteTopicAsync(topicName); } catch { }

            throw;
        }
    }
}
