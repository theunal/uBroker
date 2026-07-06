using Xunit;

namespace uBroker.UnitTests;

public class WireFormatTests
{
    [Fact]
    public void RawBinaryContentType_IsApplicationXUbrokerRaw()
    {
        Assert.Equal("application/x-ubroker-raw", WireFormat.RawBinaryContentType);
    }

    [Fact]
    public void JsonContentType_IsApplicationJson()
    {
        Assert.Equal("application/json", WireFormat.JsonContentType);
    }

    [Fact]
    public void ContentTypeHeaderKey_IsContentType()
    {
        Assert.Equal("content-type", WireFormat.ContentTypeHeaderKey);
    }

    [Fact]
    public void RawAndJson_AreDifferent()
    {
        Assert.NotEqual(WireFormat.RawBinaryContentType, WireFormat.JsonContentType);
    }

    [Fact]
    public void ContentTypeHeaderKey_IsLowercase()
    {
        Assert.Equal("content-type", WireFormat.ContentTypeHeaderKey);
        Assert.DoesNotContain("Content-Type", WireFormat.ContentTypeHeaderKey);
    }
}
