using Fluvio.Client.Abstractions;

namespace Fluvio.Client.Tests.Integration;

[Collection("Integration")]
public class AdminIntegrationTests : FluvioIntegrationTestBase
{
    [Fact]
    public async Task CreateTopic_Success()
    {
        var admin = Client!.Admin();
        var topicName = $"{GenerateTopicName()}-create";

        try
        {
            await admin.CreateTopicAsync(topicName, new TopicSpec(Partitions: 1));

            // Verify topic was created
            var topic = await admin.GetTopicAsync(topicName);
            Assert.NotNull(topic);
            Assert.Equal(topicName, topic.Name);
        }
        finally
        {
            await CleanupTopicAsync(topicName);
        }
    }

    [Fact]
    public async Task ListTopics_ReturnsTopics()
    {
        var admin = Client!.Admin();
        var topicName = await CreateTestTopicAsync();

        try
        {
            var topics = await admin.ListTopicsAsync();

            Assert.NotNull(topics);
            Assert.Contains(topics, t => t.Name == topicName);
        }
        finally
        {
            await CleanupTopicAsync(topicName);
        }
    }

    [Fact]
    public async Task GetTopic_ExistingTopic_ReturnsMetadata()
    {
        var admin = Client!.Admin();
        var topicName = await CreateTestTopicAsync(partitions: 1);

        try
        {
            var topic = await admin.GetTopicAsync(topicName);

            Assert.NotNull(topic);
            Assert.Equal(topicName, topic.Name);
            Assert.Equal(1, topic.Partitions);
        }
        finally
        {
            await CleanupTopicAsync(topicName);
        }
    }

    [Fact]
    public async Task GetTopic_NonExistentTopic_ReturnsNull()
    {
        var admin = Client!.Admin();
        var topic = await admin.GetTopicAsync("non-existent-topic");

        Assert.Null(topic);
    }

    [Fact]
    public async Task DeleteTopic_Success()
    {
        var admin = Client!.Admin();
        var topicName = await CreateTestTopicAsync();

        await admin.DeleteTopicAsync(topicName);

        // Verify topic was deleted
        var topic = await admin.GetTopicAsync(topicName);
        Assert.Null(topic);
    }
}
