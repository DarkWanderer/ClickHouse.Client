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

        Assert.Multiple(() =>
        {
            Assert.That(wrapper.CanSeek, Is.EqualTo(false));
            Assert.That(wrapper.CanRead, Is.EqualTo(true));
            Assert.That(wrapper.CanWrite, Is.EqualTo(false));
            Assert.That(wrapper.Length, Is.EqualTo(999999));
        });

        wrapper.Flush();
        streamMock.Received().Flush();

        wrapper.SetLength(123456);
        streamMock.Received().SetLength(123456);

        wrapper.Seek(1, SeekOrigin.Begin);
        streamMock.Received().Seek(1, SeekOrigin.Begin);
    }
}
