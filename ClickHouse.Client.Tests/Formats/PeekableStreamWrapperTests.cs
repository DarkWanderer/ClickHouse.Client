using System.IO;
using ClickHouse.Client.Formats;
using NSubstitute;
using NUnit.Framework;

namespace ClickHouse.Client.Tests.Formats;

public class PeekableStreamWrapperTests
{
    [Test]
    public void PeekableStreamWrapperTestSuite()
    {
        var streamMock = Substitute.For<Stream>();
        using var wrapper = new PeekableStreamWrapper(streamMock);


        streamMock.CanSeek.Returns(false);
        streamMock.CanRead.Returns(true);
        streamMock.CanWrite.Returns(false);
        streamMock.Length.Returns(999999);

        Assert.AreEqual(false, wrapper.CanSeek);
        Assert.AreEqual(true, wrapper.CanRead);
        Assert.AreEqual(false, wrapper.CanWrite);
        Assert.AreEqual(999999, wrapper.Length);

        wrapper.Flush();
        streamMock.Received().Flush();

        wrapper.SetLength(123456);
        streamMock.Received().SetLength(123456);

        wrapper.Seek(1, SeekOrigin.Begin);
        streamMock.Received().Seek(1, SeekOrigin.Begin);
    }
}
