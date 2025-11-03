using Fluvio.Client.Abstractions;

namespace Fluvio.Client.Tests.Integration;

[Collection("Integration")]
public class ConnectionIntegrationTests
{
    [Fact]
    public async Task ConnectAsync_LocalCluster_Success()
    {
        var options = new FluvioClientOptions(
            Endpoint: "localhost:9003",
            UseTls: false
        );

        await using var client = await FluvioClient.ConnectAsync(options);

        Assert.NotNull(client);
    }

    [Fact]
    public async Task ConnectAsync_WithClientId_Success()
    {
        var options = new FluvioClientOptions(
            Endpoint: "localhost:9003",
            UseTls: false,
            ClientId: "test-connection"
        );

        await using var client = await FluvioClient.ConnectAsync(options);

        Assert.NotNull(client);
    }

    [Fact]
    public async Task ConnectAsync_InvalidEndpoint_ThrowsException()
    {
        var options = new FluvioClientOptions(
            Endpoint: "localhost:9999", // Non-existent port
            UseTls: false,
            ConnectionTimeout: TimeSpan.FromSeconds(2)
        );

        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using var client = await FluvioClient.ConnectAsync(options);
        });
    }

    [Fact]
    public async Task DisposeAsync_ClosesConnection()
    {
        var options = new FluvioClientOptions(
            Endpoint: "localhost:9003",
            UseTls: false
        );

        var client = await FluvioClient.ConnectAsync(options);
        await client.DisposeAsync();

        // Attempting to use the client after disposal should fail
        Assert.Throws<InvalidOperationException>(() => client.Producer());
    }
}
