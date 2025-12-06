namespace Fluvio.Client.Tests.Integration;

/// <summary>
/// Defines the Integration test collection to ensure all integration tests run sequentially.
/// This prevents race conditions when creating/deleting topics in Fluvio.
/// </summary>
[CollectionDefinition("Integration", DisableParallelization = true)]
public class IntegrationTests
{
    // This class has no code, and is never created. Its purpose is simply
    // to be the place to apply [CollectionDefinition] and all the
    // ICollectionFixture<> interfaces.
}
