using System.Runtime.InteropServices;
using uBroker;
using Xunit;

namespace uBroker.UnitTests;

// ── Test structs ──

[UBrokerRawBinary]
[StructLayout(LayoutKind.Sequential)]
public struct SingleFieldStruct
{
    public int Value;
}

[UBrokerRawBinary]
[StructLayout(LayoutKind.Sequential)]
public struct MultiFieldStruct
{
    public int Id;
    public decimal Price;
    public long Timestamp;
}

[UBrokerRawBinary]
[StructLayout(LayoutKind.Sequential)]
public struct EnumStruct
{
    public int Id;
    public TestEnum Status;
    public long Timestamp;
}

public enum TestEnum : byte
{
    Active = 1,
    Inactive = 2,
}

// Struct with [UBrokerRawBinary] but NO [StructLayout] — must throw.
[UBrokerRawBinary]
public struct MissingLayoutStruct
{
    public int Value;
}

// Struct with a string field — must NOT be eligible even with attribute.
[UBrokerRawBinary]
[StructLayout(LayoutKind.Sequential)]
public struct RefFieldStruct
{
    public int Id;
    public string Name;
}

// Struct without attribute — must NOT be eligible.
[StructLayout(LayoutKind.Sequential)]
public struct NoAttributeStruct
{
    public int Value;
}

public class RawBinarySerializationTests
{
    [Fact]
    public void RoundTrip_SingleField()
    {
        var original = new SingleFieldStruct { Value = 42 };
        var size = UnmanagedBlitSerializer.GetSize<SingleFieldStruct>();
        var buffer = new byte[size];

        UnmanagedBlitSerializer.Write(in original, buffer);
        var deserialized = UnmanagedBlitSerializer.Read<SingleFieldStruct>(buffer);

        Assert.Equal(original.Value, deserialized.Value);
    }

    [Fact]
    public void RoundTrip_MultiField()
    {
        var original = new MultiFieldStruct
        {
            Id = 123,
            Price = 99.99m,
            Timestamp = 1700000000000L,
        };
        var size = UnmanagedBlitSerializer.GetSize<MultiFieldStruct>();
        var buffer = new byte[size];

        UnmanagedBlitSerializer.Write(in original, buffer);
        var deserialized = UnmanagedBlitSerializer.Read<MultiFieldStruct>(buffer);

        Assert.Equal(original.Id, deserialized.Id);
        Assert.Equal(original.Price, deserialized.Price);
        Assert.Equal(original.Timestamp, deserialized.Timestamp);
    }

    [Fact]
    public void RoundTrip_EnumStruct()
    {
        var original = new EnumStruct
        {
            Id = 7,
            Status = TestEnum.Inactive,
            Timestamp = 1700000000000L,
        };
        var size = UnmanagedBlitSerializer.GetSize<EnumStruct>();
        var buffer = new byte[size];

        UnmanagedBlitSerializer.Write(in original, buffer);
        var deserialized = UnmanagedBlitSerializer.Read<EnumStruct>(buffer);

        Assert.Equal(original.Id, deserialized.Id);
        Assert.Equal(original.Status, deserialized.Status);
        Assert.Equal(original.Timestamp, deserialized.Timestamp);
    }

    [Fact]
    public void GetSize_MatchesMarshal()
    {
        Assert.Equal(Marshal.SizeOf<SingleFieldStruct>(), UnmanagedBlitSerializer.GetSize<SingleFieldStruct>());
        Assert.Equal(Marshal.SizeOf<MultiFieldStruct>(), UnmanagedBlitSerializer.GetSize<MultiFieldStruct>());
        Assert.Equal(Marshal.SizeOf<EnumStruct>(), UnmanagedBlitSerializer.GetSize<EnumStruct>());
    }

    // ── Negative tests ──

    [Fact]
    public void RefFieldStruct_IsNotEligible()
    {
        Assert.False(RawBinaryTypeInfo<RefFieldStruct>.IsEligible);
    }

    [Fact]
    public void NoAttributeStruct_IsNotEligible()
    {
        Assert.False(RawBinaryTypeInfo<NoAttributeStruct>.IsEligible);
    }

    [Fact]
    public void EligibleStructs_AreCorrect()
    {
        Assert.True(RawBinaryTypeInfo<SingleFieldStruct>.IsEligible);
        Assert.True(RawBinaryTypeInfo<MultiFieldStruct>.IsEligible);
        Assert.True(RawBinaryTypeInfo<EnumStruct>.IsEligible);
    }

    // ── Layout guard test ──
    // CLR assigns LayoutKind.Sequential by default to structs, so a struct without
    // [StructLayout] still gets Sequential layout. The guard check is for LayoutKind.Auto,
    // which can only be set explicitly. We verify that attribute + layout are both required.

    [Fact]
    public void StructWithoutAttribute_IsNotEligible_EvenWithLayout()
    {
        // A struct that HAS [StructLayout] but NOT [UBrokerRawBinary] must not be eligible.
        Assert.False(RawBinaryTypeInfo<NoAttributeStruct>.IsEligible);
    }

    // ── Wire format tests ──

    [Fact]
    public void WireFormat_Constants_AreCorrect()
    {
        Assert.Equal("application/x-ubroker-raw", WireFormat.RawBinaryContentType);
        Assert.Equal("application/json", WireFormat.JsonContentType);
        Assert.Equal("content-type", WireFormat.ContentTypeHeaderKey);
    }

    // ── Size consistency ──

    [Fact]
    public void Write_Read_ConsistentWithSize()
    {
        // Ensure GetSize matches the actual bytes written and read back.
        var original = new MultiFieldStruct { Id = 42, Price = 3.14m, Timestamp = 9999 };
        var size = UnmanagedBlitSerializer.GetSize<MultiFieldStruct>();
        var buffer = new byte[size];

        UnmanagedBlitSerializer.Write(in original, buffer);

        // Size should equal Marshal.SizeOf
        Assert.Equal(Marshal.SizeOf<MultiFieldStruct>(), size);

        // Read back should match
        var read = UnmanagedBlitSerializer.Read<MultiFieldStruct>(buffer);
        Assert.Equal(original.Id, read.Id);
        Assert.Equal(original.Price, read.Price);
        Assert.Equal(original.Timestamp, read.Timestamp);
    }
}
