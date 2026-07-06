using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace uBroker;

/// <summary>
/// Span-based blit serializer for unmanaged structs. Uses Unsafe.As + Marshal
/// so callers don't need the struct constraint or unsafe context.
///
/// The absence of 'where T : struct' is intentional: it allows callers with
/// unconstrained T (like PublishAsync&lt;T&gt;) to call these methods after a
/// runtime typeof(T).IsValueType check. The methods internally verify the
/// type is a value type and throw if not.
///
/// Guarantees:
/// - Zero heap allocation on both serialize and deserialize paths.
/// - Direct memory copy — no field-by-field reflection.
/// - Caller manages buffer lifetime (ArrayPool, new byte[], etc.).
/// </summary>
internal static class UnmanagedBlitSerializer
{
    /// <summary>Get the in-memory size of T using Marshal (no struct constraint needed).</summary>
    public static int GetSize<T>() => Marshal.SizeOf(typeof(T));

    /// <summary>
    /// Write a value into the destination span. Uses Unsafe.As for zero-alloc
    /// memory reinterpretation without requiring a struct constraint.
    /// </summary>
    public static void Write<T>(in T message, Span<byte> destination)
    {
        int size = Marshal.SizeOf(typeof(T));
        ref byte src = ref Unsafe.As<T, byte>(ref Unsafe.AsRef(in message));
        ref byte dest = ref MemoryMarshal.GetReference(destination);
        Unsafe.CopyBlock(ref dest, ref src, (uint)size);
    }

    /// <summary>
    /// Read a value from the source span. Uses Unsafe.As for zero-alloc
    /// memory reinterpretation without requiring a struct constraint.
    /// </summary>
    public static T Read<T>(ReadOnlySpan<byte> source)
    {
        T result = default!;
        int size = Marshal.SizeOf(typeof(T));
        ref byte src = ref MemoryMarshal.GetReference(source);
        ref byte dest = ref Unsafe.As<T, byte>(ref result);
        Unsafe.CopyBlock(ref dest, ref src, (uint)size);
        return result;
    }
}
