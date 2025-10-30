namespace Fluvio.Client.Protocol.Records;

/// <summary>
/// Offset type alias - i64 in Rust
/// Source: fluvio-protocol/src/record/mod.rs
/// </summary>
public readonly record struct Offset(long Value)
{
    /// <summary>
    /// Implicitly converts an <see cref="Offset"/> to a <see cref="long"/>.
    /// </summary>
    public static implicit operator long(Offset offset) => offset.Value;
    /// <summary>
    /// Implicitly converts a <see cref="long"/> to an <see cref="Offset"/>.
    /// </summary>
    /// <param name="value">The long value to convert.</param>
    /// <returns>An <see cref="Offset"/> representing the specified value.</returns>
    public static implicit operator Offset(long value) => new(value);
}
