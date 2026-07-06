using System.Buffers;

namespace uBroker;

/// <summary>
/// Zero-allocation serializer interface.
/// 
/// Why IBufferWriter-based:
/// - Utf8JsonWriter writes directly into IBufferWriter, avoiding intermediate string/byte[].
/// - The provider controls buffer lifetime (ArrayPool, ArrayBufferWriter, etc.).
/// - Deserialization reads directly from ReadOnlySpan — no intermediate allocations.
/// 
/// Default implementation (Utf8JsonMessageSerializer) is provided in each provider.
/// Users can replace with custom serializers (MessagePack, Protobuf, etc.).
/// </summary>
public interface IMessageSerializer
{
    /// <summary>
    /// Serialize a message directly into the provided buffer writer.
    /// Returns the number of bytes written.
    /// </summary>
    int Serialize<T>(T message, IBufferWriter<byte> writer);

    /// <summary>
    /// Deserialize a message from a read-only byte span.
    /// Uses Span-based deserialization to avoid intermediate string allocations.
    /// </summary>
    T Deserialize<T>(ReadOnlySpan<byte> data);
}
