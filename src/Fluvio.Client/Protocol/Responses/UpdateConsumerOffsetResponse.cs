namespace Fluvio.Client.Protocol.Responses;

/// <summary>
/// Response from updating a consumer offset.
/// API Key: 1006 (UpdateConsumerOffset)
/// </summary>
internal sealed class UpdateConsumerOffsetResponse
{
    /// <summary>
    /// Error code (0 = success).
    /// </summary>
    public ErrorCode ErrorCode { get; set; }

    /// <summary>
    /// Reads the response from the binary reader.
    /// </summary>
    public static UpdateConsumerOffsetResponse ReadFrom(FluvioBinaryReader reader)
    {
        return new UpdateConsumerOffsetResponse
        {
            ErrorCode = (ErrorCode)reader.ReadInt16()
        };
    }
}
