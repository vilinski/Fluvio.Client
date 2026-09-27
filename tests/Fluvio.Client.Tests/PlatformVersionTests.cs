namespace Fluvio.Client.Tests;

/// <summary>
/// Tests for platform version compatibility checking
/// </summary>
public class PlatformVersionTests
{
    [Theory]
    [InlineData("0.9.0", "0.9.0", true)]  // Equal versions
    [InlineData("1.0.0", "0.9.0", true)]  // Cluster newer than minimum
    [InlineData("0.10.0", "0.9.0", true)] // Cluster newer (minor version)
    [InlineData("0.9.1", "0.9.0", true)]  // Cluster newer (patch version)
    [InlineData("0.8.9", "0.9.0", false)] // Cluster older
    [InlineData("0.8.0", "0.9.0", false)] // Cluster much older
    [InlineData("2.0.0", "1.0.0", true)]  // Major version upgrade
    public void VersionComparison_WorksCorrectly(string clusterVersion, string minimumVersion, bool shouldBeCompatible)
    {
        // Arrange
        var cluster = Version.Parse(clusterVersion);
        var minimum = Version.Parse(minimumVersion);

        // Act
        var isCompatible = cluster >= minimum;

        // Assert
        Assert.Equal(shouldBeCompatible, isCompatible);
    }

    [Theory]
    [InlineData("0.9.0")]
    [InlineData("1.0.0")]
    [InlineData("0.50.1")]
    public void MinimumVersion_MustBeValidSemver(string versionString)
    {
        // Act & Assert - Should not throw
        var version = Version.Parse(versionString);
        Assert.NotNull(version);
    }

    [Theory]
    [InlineData("invalid")]
    [InlineData("x.y.z")]
    public void InvalidVersionStrings_ThrowFormatException(string invalidVersion)
    {
        // Act & Assert
        Assert.ThrowsAny<Exception>(() => Version.Parse(invalidVersion));
    }

    [Fact]
    public void EmptyVersionString_ThrowsArgumentException()
    {
        // Act & Assert
        Assert.Throws<ArgumentException>(() => Version.Parse(""));
    }
}
