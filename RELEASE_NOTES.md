# Release Notes

## 1.1.0 — Raw Binary Fast-Path

**High-performance zero-allocation serialization for blittable structs.**

### New Features

- **Raw Binary Serialization Path** — Blittable structs (`int`, `decimal`, `long`, `enum`, etc.) can now bypass JSON entirely via `[UBrokerRawBinary]` attribute. Publishers and consumers automatically detect the attribute and use direct memory blit.
- **`UnmanagedBlitSerializer`** — Zero-allocation serializer using `Unsafe.As` + `Unsafe.CopyBlock`. Size cached per type via `SizeCache<T>` (no reflection on hot path).
- **`RawBinaryTypeInfo<T>`** — Per-type static eligibility cache. Checks: value type, no reference fields (`RuntimeHelpers.IsReferenceOrContainsReferences`), opt-in attribute, valid struct layout.
- **Wire Format** — `content-type: application/x-ubroker-raw` header distinguishes raw binary from JSON payloads.
- **SNS→SQS Integration Tests** — 2 new tests verifying fan-out and header propagation via LocalStack.

### How to Use

```csharp
[UBrokerRawBinary]
[StructLayout(LayoutKind.Sequential)]
public struct StockPriceEvent
{
    public int ProductId;
    public decimal Price;
    public long Timestamp;
}
```

No API changes — `PublishAsync<T>` and `Subscribe<T>` work as before.

### Constraints

- Structs must have `[StructLayout(LayoutKind.Sequential)]` or `LayoutKind.Explicit`
- No reference types (string, class, object) — falls back to JSON silently
- No schema versioning — changing struct layout breaks wire compatibility
- .NET-to-.NET only (not cross-language)
- Little-endian only (all mainstream .NET deployments)

### Bug Fixes

- **RabbitMQ BatchPublishWorker memory leak** — ArrayPool buffers now returned on both success and error paths
- **Namespace cleanup** — Removed stale `uBroker.RawBinary` namespace references across all providers and tests
- **Dead code removal** — Cleaned up commented-out code in `RawBinaryTypeInfo.cs` and `UnmanagedBlitSerializer.cs`

### Documentation

- Updated `AGENTS.md` with accurate project state (tests/samples have source files, docker-compose info)
- Updated `README.md` with correct observability metric names and local development setup
- Added `samples/uBroker.Sample.RawBinary` — demonstrates raw binary vs JSON publishing

### Test Coverage

- 27 unit tests (RawBinary serialization, eligibility, wire format)
- 14 integration tests (RabbitMQ, Kafka, Azure Service Bus, AWS SQS, SNS→SQS)
