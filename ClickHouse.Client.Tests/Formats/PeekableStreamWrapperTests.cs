using System.IO;
using ClickHouse.Client.Formats;
using NSubstitute;

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

        ClassicAssert.AreEqual(false, wrapper.CanSeek);
        ClassicAssert.AreEqual(true, wrapper.CanRead);
        ClassicAssert.AreEqual(false, wrapper.CanWrite);
        ClassicAssert.AreEqual(999999, wrapper.Length);

        wrapper.Flush();
        streamMock.Received().Flush();

        wrapper.SetLength(123456);
        streamMock.Received().SetLength(123456);

        wrapper.Seek(1, SeekOrigin.Begin);
        streamMock.Received().Seek(1, SeekOrigin.Begin);
    }
}
