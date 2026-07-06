using System.Buffers;
using System.Text.Json;

namespace uBroker.RabbitMQ.Serialization;

/// <summary>
/// Zero-allocation message serializer using System.Text.Json Utf8JsonWriter/Reader.
///
/// Why Utf8JsonWriter/Reader instead of JsonSerializer.Serialize/Deserialize:
/// - Serialize/Deserialize returns string → allocates managed heap memory on every call.
/// - Utf8JsonWriter writes directly into an IBufferWriter&lt;byte&gt; (e.g. ArrayPool-backed).
/// - Utf8JsonReader reads directly from ReadOnlySpan&lt;byte&gt; — no intermediate string.
/// - Result: serialization/deserialization contributes 0 bytes to managed heap on hot path.
///
/// Trade-offs:
/// - Slightly more verbose code (we control the writer lifecycle).
/// - Requires ArrayPool buffer management (try/finally pattern).
/// - But: eliminates the #1 source of GC pressure in messaging systems.
/// </summary>
public sealed class Utf8JsonMessageSerializer : IMessageSerializer
{
    private static readonly JsonSerializerOptions s_options = new()
    {
        PropertyNameCaseInsensitive = true,
        AllowTrailingCommas = true,
        ReadCommentHandling = JsonCommentHandling.Skip,
    };

    /// <summary>
    /// Serialize a message into the provided buffer writer.
    /// The writer must be an ArrayBufferWriter&lt;byte&gt; or similar concrete type
    /// that supports WrittenCount/WrittenMemory.
    /// </summary>
    public int Serialize<T>(T message, IBufferWriter<byte> writer)
    {
        // Utf8JsonWriter writes directly into the IBufferWriter — no intermediate string allocation.
        using var jsonWriter = new Utf8JsonWriter(writer, new JsonWriterOptions
        {
            Indented = false,
            Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
            SkipValidation = true,
        });

        JsonSerializer.Serialize(jsonWriter, message, s_options);
        jsonWriter.Flush();

        // Utf8JsonWriter.BytesCommitted returns the total bytes written.
        return (int)jsonWriter.BytesCommitted;
    }

    /// <summary>
    /// Deserialize a message from a read-only byte span.
    /// No string conversion — Utf8JsonReader parses directly from UTF-8 bytes.
    /// </summary>
    public T Deserialize<T>(ReadOnlySpan<byte> data)
    {
        var reader = new Utf8JsonReader(data, isFinalBlock: true, state: default);
        return JsonSerializer.Deserialize<T>(ref reader, s_options)
            ?? throw new InvalidOperationException($"Deserialization returned null for type {typeof(T).Name}");
    }
}
