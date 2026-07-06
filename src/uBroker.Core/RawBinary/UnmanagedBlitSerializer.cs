using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace uBroker;

/// <summary>
/// Span-based blit serializer for unmanaged structs. Uses Unsafe.As + Marshal
/// for zero-allocation memory reinterpretation.
///
/// Guarantees:
/// - Zero heap allocation on both serialize and deserialize paths.
/// - Direct memory copy — no field-by-field reflection.
/// - Size cached per type via SizeCache&lt;T&gt; (JIT static constructor).
/// - Caller manages buffer lifetime (ArrayPool, new byte[], etc.).
/// </summary>
/// 
internal static class UnmanagedBlitSerializer
{
    public static int GetSize<T>() => SizeCache<T>.Value;

    public static void Write<T>(in T message, Span<byte> destination)
    {
        int size = SizeCache<T>.Value;
        if (destination.Length < size)
            ThrowBufferTooSmall(destination.Length, size);

        ref byte src = ref Unsafe.As<T, byte>(ref Unsafe.AsRef(in message));
        ref byte dest = ref MemoryMarshal.GetReference(destination);
        Unsafe.CopyBlock(ref dest, ref src, (uint)size);
    }

    public static T Read<T>(ReadOnlySpan<byte> source)
    {
        int size = SizeCache<T>.Value;
        if (source.Length < size)
            ThrowBufferTooSmall(source.Length, size);

        T result = default!;
        ref byte src = ref MemoryMarshal.GetReference(source);
        ref byte dest = ref Unsafe.As<T, byte>(ref result);
        Unsafe.CopyBlock(ref dest, ref src, (uint)size);
        return result;
    }

    private static class SizeCache<T>
    {
        public static readonly int Value = Marshal.SizeOf<T>();
    }

    [DoesNotReturn]
    private static void ThrowBufferTooSmall(int actual, int expected) =>
        throw new ArgumentException($"Buffer too small. Required: {expected}, Actual: {actual}");
}