using System.Buffers;
using System.Text.Json;

namespace uBroker.Aws.Serialization;

/// <summary>
/// Zero-allocation JSON serializer for AWS providers.
/// </summary>
public sealed class Utf8JsonMessageSerializer : IMessageSerializer
{
    private static readonly JsonSerializerOptions s_options = new()
    {
        PropertyNameCaseInsensitive = true,
        AllowTrailingCommas = true,
        ReadCommentHandling = JsonCommentHandling.Skip,
    };

    public int Serialize<T>(T message, IBufferWriter<byte> writer)
    {
        using var jsonWriter = new Utf8JsonWriter(writer, new JsonWriterOptions
        {
            Indented = false,
            Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
            SkipValidation = true,
        });

        JsonSerializer.Serialize(jsonWriter, message, s_options);
        jsonWriter.Flush();
        return (int)jsonWriter.BytesCommitted;
    }

    public T Deserialize<T>(ReadOnlySpan<byte> data)
    {
        var reader = new Utf8JsonReader(data, isFinalBlock: true, state: default);
        return JsonSerializer.Deserialize<T>(ref reader, s_options)
            ?? throw new InvalidOperationException($"Deserialization returned null for type {typeof(T).Name}");
    }
}
