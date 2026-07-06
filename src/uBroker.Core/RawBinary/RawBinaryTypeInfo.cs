using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace uBroker;

/// <summary>
/// Per-type static cache for raw binary eligibility. Computes once per T
/// via CLR type-initialization (thread-safe, no lock needed).
///
/// No struct constraint on T — eligibility is checked at runtime via
/// typeof(T).IsValueType. This allows callers with unconstrained T
/// (like PublishAsync&lt;T&gt;) to check eligibility without compilation errors.
///
/// Eligibility rules:
/// 1. T must be a value type (struct) — checked at runtime.
/// 2. T must not contain reference types — checked via field inspection.
/// 3. T must have [UBrokerRawBinary] attribute (explicit opt-in).
/// 4. T must have [StructLayout(LayoutKind.Sequential)] or Explicit.
/// </summary>
internal static class RawBinaryTypeInfo<T>
{
    public static readonly bool IsEligible = Compute();

    private static bool Compute()
    {
        var type = typeof(T);

        if (!type.IsValueType)
            return false;

        if (ContainsReferenceFields(type))
            return false;

        if (type.GetCustomAttribute<UBrokerRawBinaryAttribute>() is null)
            return false;

        var layout = type.StructLayoutAttribute;
        if (layout is null || layout.Value == LayoutKind.Auto)
        {
            throw new InvalidOperationException(
                $"{type.Name} is marked with [UBrokerRawBinary] but does not define " +
                $"[StructLayout(LayoutKind.Sequential)] or [StructLayout(LayoutKind.Explicit)]. " +
                $"A deterministic memory layout is required for raw binary serialization.");
        }

        return true;
    }

    private static bool ContainsReferenceFields(Type type)
    {
        var fields = type.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
        foreach (var field in fields)
        {
            var ft = field.FieldType;
            if (ft.IsClass || ft.IsInterface || ft.IsPointer || ft.IsByRef)
                return true;
            if (ft.IsEnum)
            {
                var underlying = Enum.GetUnderlyingType(ft);
                if (underlying == typeof(nint) || underlying == typeof(nuint))
                    return true;
            }
        }
        return false;
    }
}
