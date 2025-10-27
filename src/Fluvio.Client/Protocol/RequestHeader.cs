namespace Fluvio.Client.Protocol;

/// <summary>
/// Fluvio request header
/// </summary>
internal sealed class RequestHeader
{
    public ApiKey ApiKey { get; set; }
    public short ApiVersion { get; set; }
    public int CorrelationId { get; set; }
    public string? ClientId { get; set; }

    public void WriteTo(FluvioBinaryWriter writer)
    {
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteString(ClientId);
    }

    public static RequestHeader ReadFrom(FluvioBinaryReader reader)
    {
        return new RequestHeader
        {
            ApiKey = (ApiKey)reader.ReadInt16(),
            ApiVersion = reader.ReadInt16(),
            CorrelationId = reader.ReadInt32(),
            ClientId = reader.ReadString()
        };
    }
}

/// <summary>
/// Fluvio response header
/// </summary>
internal sealed class ResponseHeader
{
    public int CorrelationId { get; set; }

    public void WriteTo(FluvioBinaryWriter writer)
    {
        writer.WriteInt32(CorrelationId);
    }

    public static ResponseHeader ReadFrom(FluvioBinaryReader reader)
    {
        return new ResponseHeader
        {
            CorrelationId = reader.ReadInt32()
        };
    }
}
