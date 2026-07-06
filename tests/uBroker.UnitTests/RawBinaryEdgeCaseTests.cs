using System.Runtime.InteropServices;
using uBroker;
using Xunit;

namespace uBroker.UnitTests;

// ── Edge case structs ──

[UBrokerRawBinary]
[StructLayout(LayoutKind.Sequential)]
public struct BoolStruct
{
    public bool Flag;
    public int Value;
}

[UBrokerRawBinary]
[StructLayout(LayoutKind.Sequential)]
public struct ByteStruct
{
    public byte A;
    public byte B;
    public byte C;
    public byte D;
}

[UBrokerRawBinary]
[StructLayout(LayoutKind.Sequential)]
public struct DoubleStruct
{
    public double Lat;
    public double Lon;
}

[UBrokerRawBinary]
[StructLayout(LayoutKind.Sequential)]
public struct GuidStruct
{
    public Guid Id;
    public long Timestamp;
}

[UBrokerRawBinary]
[StructLayout(LayoutKind.Sequential)]
public struct NestedEnumStruct
{
    public int Id;
    public TestEnum Status;
    public long Timestamp;
}

// Struct with a class field — should NOT be eligible.
[UBrokerRawBinary]
[StructLayout(LayoutKind.Sequential)]
public struct ClassFieldStruct
{
    public int Id;
    public object? Obj;
}

public class RawBinaryEdgeCaseTests
{
    [Fact]
    public void RoundTrip_BoolStruct()
    {
        var original = new BoolStruct { Flag = true, Value = -42 };
        var buffer = new byte[UnmanagedBlitSerializer.GetSize<BoolStruct>()];

        UnmanagedBlitSerializer.Write(in original, buffer);
        var result = UnmanagedBlitSerializer.Read<BoolStruct>(buffer);

        Assert.Equal(original.Flag, result.Flag);
        Assert.Equal(original.Value, result.Value);
    }

    [Fact]
    public void RoundTrip_ByteStruct()
    {
        var original = new ByteStruct { A = 0, B = 128, C = 255, D = 42 };
        var buffer = new byte[UnmanagedBlitSerializer.GetSize<ByteStruct>()];

        UnmanagedBlitSerializer.Write(in original, buffer);
        var result = UnmanagedBlitSerializer.Read<ByteStruct>(buffer);

        Assert.Equal(original.A, result.A);
        Assert.Equal(original.B, result.B);
        Assert.Equal(original.C, result.C);
        Assert.Equal(original.D, result.D);
    }

    [Fact]
    public void RoundTrip_DoubleStruct()
    {
        var original = new DoubleStruct { Lat = 41.0082, Lon = 28.9784 };
        var buffer = new byte[UnmanagedBlitSerializer.GetSize<DoubleStruct>()];

        UnmanagedBlitSerializer.Write(in original, buffer);
        var result = UnmanagedBlitSerializer.Read<DoubleStruct>(buffer);

        Assert.Equal(original.Lat, result.Lat);
        Assert.Equal(original.Lon, result.Lon);
    }

    [Fact]
    public void RoundTrip_GuidStruct()
    {
        var guid = Guid.NewGuid();
        var original = new GuidStruct { Id = guid, Timestamp = 1234567890L };
        var buffer = new byte[UnmanagedBlitSerializer.GetSize<GuidStruct>()];

        UnmanagedBlitSerializer.Write(in original, buffer);
        var result = UnmanagedBlitSerializer.Read<GuidStruct>(buffer);

        Assert.Equal(original.Id, result.Id);
        Assert.Equal(original.Timestamp, result.Timestamp);
    }

    [Fact]
    public void RoundTrip_NestedEnumStruct()
    {
        var original = new NestedEnumStruct
        {
            Id = 1,
            Status = TestEnum.Active,
            Timestamp = 9999999L,
        };
        var buffer = new byte[UnmanagedBlitSerializer.GetSize<NestedEnumStruct>()];

        UnmanagedBlitSerializer.Write(in original, buffer);
        var result = UnmanagedBlitSerializer.Read<NestedEnumStruct>(buffer);

        Assert.Equal(original.Id, result.Id);
        Assert.Equal(original.Status, result.Status);
        Assert.Equal(original.Timestamp, result.Timestamp);
    }

    [Fact]
    public void ClassFieldStruct_IsNotEligible()
    {
        Assert.False(RawBinaryTypeInfo<ClassFieldStruct>.IsEligible);
    }

    [Fact]
    public void BoolStruct_IsEligible()
    {
        Assert.True(RawBinaryTypeInfo<BoolStruct>.IsEligible);
    }

    [Fact]
    public void GuidStruct_IsEligible()
    {
        Assert.True(RawBinaryTypeInfo<GuidStruct>.IsEligible);
    }

    [Fact]
    public void Write_OverwritesBuffer_Completely()
    {
        // Ensure previous data in buffer is fully overwritten.
        var buffer = new byte[100];
        Array.Fill(buffer, (byte)0xFF);

        var original = new SingleFieldStruct { Value = 42 };
        var size = UnmanagedBlitSerializer.GetSize<SingleFieldStruct>();
        UnmanagedBlitSerializer.Write(in original, buffer);

        var result = UnmanagedBlitSerializer.Read<SingleFieldStruct>(buffer);
        Assert.Equal(42, result.Value);

        // Bytes after the struct should still be 0xFF (not touched).
        for (int i = size; i < buffer.Length; i++)
        {
            Assert.Equal(0xFF, buffer[i]);
        }
    }

    [Fact]
    public void MultipleStructs_InSameBuffer()
    {
        // Write two structs side by side in one buffer.
        var s1 = new SingleFieldStruct { Value = 11 };
        var s2 = new SingleFieldStruct { Value = 22 };
        var size = UnmanagedBlitSerializer.GetSize<SingleFieldStruct>();
        var buffer = new byte[size * 2];

        UnmanagedBlitSerializer.Write(in s1, buffer.AsSpan(0, size));
        UnmanagedBlitSerializer.Write(in s2, buffer.AsSpan(size, size));

        var r1 = UnmanagedBlitSerializer.Read<SingleFieldStruct>(buffer.AsSpan(0, size));
        var r2 = UnmanagedBlitSerializer.Read<SingleFieldStruct>(buffer.AsSpan(size, size));

        Assert.Equal(11, r1.Value);
        Assert.Equal(22, r2.Value);
    }

    [Fact]
    public void LargeDecimal_RoundTrips()
    {
        var original = new MultiFieldStruct
        {
            Id = int.MaxValue,
            Price = 9999999999.99m,
            Timestamp = long.MaxValue,
        };
        var buffer = new byte[UnmanagedBlitSerializer.GetSize<MultiFieldStruct>()];

        UnmanagedBlitSerializer.Write(in original, buffer);
        var result = UnmanagedBlitSerializer.Read<MultiFieldStruct>(buffer);

        Assert.Equal(original.Id, result.Id);
        Assert.Equal(original.Price, result.Price);
        Assert.Equal(original.Timestamp, result.Timestamp);
    }

    [Fact]
    public void NegativeDecimal_RoundTrips()
    {
        var original = new MultiFieldStruct
        {
            Id = -1,
            Price = -123.45m,
            Timestamp = -999L,
        };
        var buffer = new byte[UnmanagedBlitSerializer.GetSize<MultiFieldStruct>()];

        UnmanagedBlitSerializer.Write(in original, buffer);
        var result = UnmanagedBlitSerializer.Read<MultiFieldStruct>(buffer);

        Assert.Equal(original.Id, result.Id);
        Assert.Equal(original.Price, result.Price);
        Assert.Equal(original.Timestamp, result.Timestamp);
    }
}
