namespace uBroker;

/// <summary>
/// Opt-in marker for raw binary serialization. Apply to blittable structs
/// that should bypass JSON and be serialized via direct memory blit.
///
/// Requirements for raw binary eligibility:
/// - Struct must be unmanaged (no reference fields).
/// - Struct must have [StructLayout(LayoutKind.Sequential)] or Explicit.
/// - Both producer and consumer must be .NET/uBroker (not cross-language).
/// - Platform must be little-endian (all mainstream .NET deployments).
///
/// Wire format: the struct's raw bytes are written directly — no schema,
/// no field names, no length prefixes. This means:
/// - No schema versioning (changing the struct breaks wire compat).
/// - No cross-language interop (only .NET-to-.NET).
/// - Byte layout depends on runtime's struct packing (StructLayout guarantees this).
/// </summary>
[AttributeUsage(AttributeTargets.Struct, Inherited = false)]
public sealed class UBrokerRawBinaryAttribute : Attribute { }