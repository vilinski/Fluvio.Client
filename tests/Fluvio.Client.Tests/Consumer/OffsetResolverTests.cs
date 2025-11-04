using Fluvio.Client.Abstractions;
using Fluvio.Client.Consumer;
using Xunit;

namespace Fluvio.Client.Tests.Consumer;

/// <summary>
/// Unit tests for OffsetResolver utility
/// </summary>
public class OffsetResolverTests
{
    [Fact]
    public void ResolveStartOffset_ExplicitOffset_ReturnsExplicitValue()
    {
        // Arrange
        var storedOffset = 100L;
        var explicitOffset = 50L;

        // Act
        var result = OffsetResolver.ResolveStartOffset(storedOffset, OffsetResetStrategy.Latest, explicitOffset);

        // Assert
        Assert.Equal(50L, result);
    }

    [Fact]
    public void ResolveStartOffset_Earliest_ReturnsBeginningOffset()
    {
        // Act
        var result = OffsetResolver.ResolveStartOffset(null, OffsetResetStrategy.Earliest);

        // Assert
        Assert.Equal(OffsetResolver.BeginningOffset, result);
    }

    [Fact]
    public void ResolveStartOffset_Latest_ReturnsEndOffset()
    {
        // Act
        var result = OffsetResolver.ResolveStartOffset(null, OffsetResetStrategy.Latest);

        // Assert
        Assert.Equal(OffsetResolver.EndOffset, result);
    }

    [Fact]
    public void ResolveStartOffset_StoredOrEarliest_WithStoredOffset_ReturnsStoredPlusOne()
    {
        // Arrange
        var storedOffset = 99L;

        // Act
        var result = OffsetResolver.ResolveStartOffset(storedOffset, OffsetResetStrategy.StoredOrEarliest);

        // Assert
        Assert.Equal(100L, result); // Stored + 1 (next message after committed)
    }

    [Fact]
    public void ResolveStartOffset_StoredOrEarliest_WithoutStoredOffset_ReturnsEarliest()
    {
        // Act
        var result = OffsetResolver.ResolveStartOffset(null, OffsetResetStrategy.StoredOrEarliest);

        // Assert
        Assert.Equal(OffsetResolver.BeginningOffset, result);
    }

    [Fact]
    public void ResolveStartOffset_StoredOrLatest_WithStoredOffset_ReturnsStoredPlusOne()
    {
        // Arrange
        var storedOffset = 99L;

        // Act
        var result = OffsetResolver.ResolveStartOffset(storedOffset, OffsetResetStrategy.StoredOrLatest);

        // Assert
        Assert.Equal(100L, result);
    }

    [Fact]
    public void ResolveStartOffset_StoredOrLatest_WithoutStoredOffset_ReturnsLatest()
    {
        // Act
        var result = OffsetResolver.ResolveStartOffset(null, OffsetResetStrategy.StoredOrLatest);

        // Assert
        Assert.Equal(OffsetResolver.EndOffset, result);
    }

    [Fact]
    public void GetConsumerId_NoConsumerGroup_ReturnsNull()
    {
        // Act
        var result = OffsetResolver.GetConsumerId(null);

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public void GetConsumerId_WithConsumerGroupAndInstance_ReturnsCombinedId()
    {
        // Act
        var result = OffsetResolver.GetConsumerId("my-group", "instance-1");

        // Assert
        Assert.Equal("my-group-instance-1", result);
    }

    [Fact]
    public void GetConsumerId_WithConsumerGroupNoInstance_ReturnsGeneratedId()
    {
        // Act
        var result = OffsetResolver.GetConsumerId("my-group");

        // Assert
        Assert.NotNull(result);
        Assert.StartsWith("my-group-", result);
        Assert.Equal("my-group-".Length + 8, result.Length); // group + dash + 8 char GUID
    }

    [Fact]
    public void GetConsumerId_WithEmptyConsumerGroup_ReturnsNull()
    {
        // Act
        var result = OffsetResolver.GetConsumerId(string.Empty);

        // Assert
        Assert.Null(result);
    }

    [Theory]
    [InlineData(OffsetResetStrategy.Earliest, 0L)]
    [InlineData(OffsetResetStrategy.Latest, -1L)]
    [InlineData(OffsetResetStrategy.StoredOrEarliest, 0L)]
    [InlineData(OffsetResetStrategy.StoredOrLatest, -1L)]
    public void ResolveStartOffset_NoStoredOffset_ReturnsExpectedDefault(OffsetResetStrategy strategy, long expected)
    {
        // Act
        var result = OffsetResolver.ResolveStartOffset(null, strategy);

        // Assert
        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData(0L, 1L)]
    [InlineData(99L, 100L)]
    [InlineData(999L, 1000L)]
    public void ResolveStartOffset_StoredStrategy_IncrementsStoredOffset(long storedOffset, long expected)
    {
        // Act - StoredOrEarliest
        var result1 = OffsetResolver.ResolveStartOffset(storedOffset, OffsetResetStrategy.StoredOrEarliest);
        // Act - StoredOrLatest
        var result2 = OffsetResolver.ResolveStartOffset(storedOffset, OffsetResetStrategy.StoredOrLatest);

        // Assert
        Assert.Equal(expected, result1);
        Assert.Equal(expected, result2);
    }
}
