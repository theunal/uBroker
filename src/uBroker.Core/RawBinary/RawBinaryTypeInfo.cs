using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace uBroker.RawBinary;

internal static class RawBinaryTypeInfo<T>
{
    public static readonly bool IsEligible = Compute();

    private static bool Compute()
    {
        var type = typeof(T);

        // 1. Value type olmalı
        if (!type.IsValueType)
            return false;

        // 2. Referans içermemeli (intrinsic, reflection yok)
        if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            return false;

        // 3. Opt-in attribute zorunlu
        if (type.GetCustomAttribute<UBrokerRawBinaryAttribute>() is null)
            return false;

        // 4. Layout kontrolü (Sequential veya Explicit)
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
}