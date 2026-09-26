using Fluvio.Client.Abstractions;

namespace Fluvio.Client.Tests.Integration;

[Collection("Integration")]
public class ConnectionIntegrationTests
{
    [Fact]
    public async Task ConnectAsync_ConfiguredCluster_Success()
    {
        var options = IntegrationTestConfig.Create();

        await using var client = await FluvioClient.ConnectAsync(options);

        Assert.NotNull(client);
    }

    [Fact]
    public async Task ConnectAsync_WithClientId_Success()
    {
        var options = IntegrationTestConfig.Create("test-connection");

        await using var client = await FluvioClient.ConnectAsync(options);

        Assert.NotNull(client);
    }

    [Fact]
    public async Task ConnectAsync_InvalidEndpoint_ThrowsException()
    {
        var options = new FluvioClientOptions(
            SpuEndpoint: "localhost:9999", // Non-existent port
            ScEndpoint: "localhost:9998",
            UseTls: false,
            ConnectionTimeout: TimeSpan.FromSeconds(2)
        );

        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using var client = await FluvioClient.ConnectAsync(options);
        });
    }

    [Fact]
    public async Task ConnectAsync_MissingProfile_DoesNotFallBackToExplicitEndpoint()
    {
        var options = IntegrationTestConfig.Create() with
        {
            Profile = $"missing-{Guid.NewGuid():N}",
            ScEndpoint = "localhost:9003"
        };

        var error = await Assert.ThrowsAnyAsync<FluvioException>(() => FluvioClient.ConnectAsync(options));
        Assert.Contains(options.Profile, error.Message);
    }

    [Fact]
    public async Task DisposeAsync_ClosesConnection()
    {
        var options = IntegrationTestConfig.Create();

        var client = await FluvioClient.ConnectAsync(options);
        await client.DisposeAsync();

        // Attempting to use the client after disposal should fail
        Assert.Throws<InvalidOperationException>(() => client.Producer());
    }
}
