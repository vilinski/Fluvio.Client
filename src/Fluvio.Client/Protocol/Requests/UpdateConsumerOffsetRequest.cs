namespace Fluvio.Client.Protocol.Requests;

/// <summary>
/// Request to update (commit) a consumer offset to the cluster.
/// API Key: 1006 (UpdateConsumerOffset)
/// </summary>
internal sealed class UpdateConsumerOffsetRequest
{
    /// <summary>
    /// Offset to commit.
    /// </summary>
    public long Offset { get; set; }

    /// <summary>
    /// Stream session ID (obtained from StreamFetch response).
    /// Used to associate offset with an active streaming session.
    /// </summary>
    public uint SessionId { get; set; }

    /// <summary>
    /// Writes the request to the binary writer.
    /// </summary>
    public void WriteTo(FluvioBinaryWriter writer)
    {
        writer.WriteInt64(Offset);
        writer.WriteUInt32(SessionId);
    }
}
