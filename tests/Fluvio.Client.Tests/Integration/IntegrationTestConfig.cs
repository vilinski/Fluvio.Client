using Fluvio.Client.Abstractions;

namespace Fluvio.Client.Tests.Integration;

internal static class IntegrationTestConfig
{
    internal static FluvioClientOptions Create(string clientId = "integration-test")
    {
        var profile = Environment.GetEnvironmentVariable("FLUVIO_TEST_PROFILE");
        return string.IsNullOrWhiteSpace(profile)
            ? new FluvioClientOptions(ScEndpoint: "localhost:9003", UseTls: false, ClientId: clientId)
            : new FluvioClientOptions(Profile: profile, ClientId: clientId);
    }
}
