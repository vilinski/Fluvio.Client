using Fluvio.Client.Protocol;
using Xunit;

namespace Fluvio.Client.Tests.Protocol;

public class RequestHeaderTests
{
    [Fact]
    public void RequestHeader_RoundTrip()
    {
        var header = new RequestHeader
        {
            ApiKey = ApiKey.Produce,
            ApiVersion = 1,
            CorrelationId = 42,
            ClientId = "test-client"
        };

        using var writer = new FluvioBinaryWriter();
        header.WriteTo(writer);

        var bytes = writer.ToArray();

        using var reader = new FluvioBinaryReader(bytes);
        var readHeader = RequestHeader.ReadFrom(reader);

        Assert.Equal(header.ApiKey, readHeader.ApiKey);
        Assert.Equal(header.ApiVersion, readHeader.ApiVersion);
        Assert.Equal(header.CorrelationId, readHeader.CorrelationId);
        Assert.Equal(header.ClientId, readHeader.ClientId);
    }

    [Fact]
    public void RequestHeader_NullClientId()
    {
        var header = new RequestHeader
        {
            ApiKey = ApiKey.Fetch,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = null
        };

        using var writer = new FluvioBinaryWriter();
        header.WriteTo(writer);

        var bytes = writer.ToArray();

        using var reader = new FluvioBinaryReader(bytes);
        var readHeader = RequestHeader.ReadFrom(reader);

        Assert.Null(readHeader.ClientId);
    }

    [Fact]
    public void ResponseHeader_RoundTrip()
    {
        var header = new ResponseHeader
        {
            CorrelationId = 123
        };

        using var writer = new FluvioBinaryWriter();
        header.WriteTo(writer);

        var bytes = writer.ToArray();

        using var reader = new FluvioBinaryReader(bytes);
        var readHeader = ResponseHeader.ReadFrom(reader);

        Assert.Equal(header.CorrelationId, readHeader.CorrelationId);
    }
}
