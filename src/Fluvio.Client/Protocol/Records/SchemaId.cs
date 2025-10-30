namespace Fluvio.Client.Protocol.Records;

/// <summary>
/// Schema ID type
/// Source: fluvio-protocol/src/record/batch.rs
/// </summary>
public readonly record struct SchemaId(uint Value)
{
    /// <summary>
    /// Implicitly converts SchemaId to uint.
    /// </summary>
    public static implicit operator uint(SchemaId schemaId) => schemaId.Value;

    /// <summary>
    /// Implicitly converts uint to SchemaId.
    /// </summary>
    public static implicit operator SchemaId(uint value) => new(value);

    internal void Encode(FluvioBinaryWriter writer) => writer.WriteUInt32(Value);

    internal static SchemaId Decode(FluvioBinaryReader reader) => new(reader.ReadUInt32());
}
