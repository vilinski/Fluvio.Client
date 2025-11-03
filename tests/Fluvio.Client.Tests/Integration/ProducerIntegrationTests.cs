using System.Text;
using Fluvio.Client.Abstractions;

namespace Fluvio.Client.Tests.Integration;

[Collection("Integration")]
public class ProducerIntegrationTests : FluvioIntegrationTestBase
{
    [Fact]
    public async Task SendAsync_SingleMessage_Success()
    {
        var topicName = await CreateTestTopicAsync();
        var producer = Client!.Producer();

        try
        {
            var message = Encoding.UTF8.GetBytes("Hello, Fluvio!");
            var offset = await producer.SendAsync(topicName, message);

            Assert.True(offset >= 0);
        }
        finally
        {
            await CleanupTopicAsync(topicName);
        }
    }

    [Fact]
    public async Task SendAsync_WithKey_Success()
    {
        var topicName = await CreateTestTopicAsync();
        var producer = Client!.Producer();

        try
        {
            var key = Encoding.UTF8.GetBytes("key-1");
            var value = Encoding.UTF8.GetBytes("value-1");

            var offset = await producer.SendAsync(topicName, value, key);

            Assert.True(offset >= 0);
        }
        finally
        {
            await CleanupTopicAsync(topicName);
        }
    }

    [Fact]
    public async Task SendAsync_MultipleMessages_IncreasingOffsets()
    {
        var topicName = await CreateTestTopicAsync();
        var producer = Client!.Producer();

        try
        {
            var offset1 = await producer.SendAsync(topicName, Encoding.UTF8.GetBytes("msg1"));
            var offset2 = await producer.SendAsync(topicName, Encoding.UTF8.GetBytes("msg2"));
            var offset3 = await producer.SendAsync(topicName, Encoding.UTF8.GetBytes("msg3"));

            Assert.True(offset2 > offset1);
            Assert.True(offset3 > offset2);
        }
        finally
        {
            await CleanupTopicAsync(topicName);
        }
    }

    [Fact]
    public async Task SendBatchAsync_MultpleMessages_Success()
    {
        var topicName = await CreateTestTopicAsync();
        var producer = Client!.Producer();

        try
        {
            var records = new List<ProduceRecord>
            {
                new(Encoding.UTF8.GetBytes("batch-1")),
                new(Encoding.UTF8.GetBytes("batch-2")),
                new(Encoding.UTF8.GetBytes("batch-3"))
            };

            var offsets = await producer.SendBatchAsync(topicName, records);

            Assert.Equal(3, offsets.Count);
            Assert.True(offsets[0] >= 0);
            Assert.Equal(offsets[0] + 1, offsets[1]);
            Assert.Equal(offsets[1] + 1, offsets[2]);
        }
        finally
        {
            await CleanupTopicAsync(topicName);
        }
    }

    [Fact]
    public async Task SendAsync_LargeMessage_Success()
    {
        var topicName = await CreateTestTopicAsync();
        var producer = Client!.Producer();

        try
        {
            // 1MB message
            var largeMessage = new byte[1024 * 1024];
            Array.Fill<byte>(largeMessage, 42);

            var offset = await producer.SendAsync(topicName, largeMessage);

            Assert.True(offset >= 0);
        }
        finally
        {
            await CleanupTopicAsync(topicName);
        }
    }

    [Fact]
    public async Task SendAsync_EmptyMessage_Success()
    {
        var topicName = await CreateTestTopicAsync();
        var producer = Client!.Producer();

        try
        {
            var offset = await producer.SendAsync(topicName, Array.Empty<byte>());

            Assert.True(offset >= 0);
        }
        finally
        {
            await CleanupTopicAsync(topicName);
        }
    }
}
