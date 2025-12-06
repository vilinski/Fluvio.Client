using Fluvio.Client.Abstractions;

namespace Fluvio.Client.Tests.Producer;

/// <summary>
/// Tests for producer batch flush control behavior
/// </summary>
public class BatchFlushControlTests
{
    [Fact]
    public void ProducerOptions_DefaultValues_AreCorrect()
    {
        // Arrange & Act
        var options = new ProducerOptions();

        // Assert
        Assert.Equal(1000, options.BatchSize);
        Assert.Equal(TimeSpan.FromMilliseconds(100), options.LingerTime);
        Assert.Equal(1024 * 1024, options.MaxRequestSize);
        Assert.Equal(TimeSpan.FromSeconds(5), options.Timeout);
    }

    [Fact]
    public void ProducerOptions_CustomBatchSize_IsRespected()
    {
        // Arrange & Act
        var options = new ProducerOptions(BatchSize: 500);

        // Assert
        Assert.Equal(500, options.BatchSize);
        Assert.Equal(TimeSpan.FromMilliseconds(100), options.LingerTime);
    }

    [Fact]
    public void ProducerOptions_CustomLingerTime_IsRespected()
    {
        // Arrange & Act
        var options = new ProducerOptions(LingerTime: TimeSpan.FromMilliseconds(50));

        // Assert
        Assert.Equal(1000, options.BatchSize);
        Assert.Equal(TimeSpan.FromMilliseconds(50), options.LingerTime);
    }

    [Fact]
    public void ProducerOptions_DefaultLingerTime_WhenZeroProvided_UsesDefault()
    {
        // Arrange & Act - TimeSpan.Zero is treated as "not specified" and gets default value
        var options = new ProducerOptions(LingerTime: TimeSpan.Zero);

        // Assert - Gets default 100ms instead of zero
        Assert.Equal(TimeSpan.FromMilliseconds(100), options.LingerTime);
    }

    [Fact]
    public void ProducerOptions_AllCustomValues_AreRespected()
    {
        // Arrange & Act
        var options = new ProducerOptions(
            BatchSize: 100,
            MaxRequestSize: 512 * 1024,
            LingerTime: TimeSpan.FromMilliseconds(200),
            Timeout: TimeSpan.FromSeconds(10));

        // Assert
        Assert.Equal(100, options.BatchSize);
        Assert.Equal(512 * 1024, options.MaxRequestSize);
        Assert.Equal(TimeSpan.FromMilliseconds(200), options.LingerTime);
        Assert.Equal(TimeSpan.FromSeconds(10), options.Timeout);
    }
}
