using System.IO;
using ClickHouse.Client.Formats;
using Moq;
using NUnit.Framework;

namespace ClickHouse.Client.Tests.Formats
{
    public class PeekableStreamWrapperTests
    {
        [Test]
        public void PeekableStreamWrapperTestSuite()
        {
            var streamMock = new Mock<Stream>();
            using var wrapper = new PeekableStreamWrapper(streamMock.Object);

            streamMock.Setup(x => x.CanRead).Returns(true);
            Assert.AreEqual(true, wrapper.CanRead);

            streamMock.Setup(x => x.CanWrite).Returns(false);
            Assert.AreEqual(false, wrapper.CanWrite);

            streamMock.Setup(x => x.CanSeek).Returns(false);
            Assert.AreEqual(false, wrapper.CanSeek);

            streamMock.Setup(x => x.Length).Returns(999999);
            Assert.AreEqual(999999, wrapper.Length);

            streamMock.Setup(x => x.Flush());
            wrapper.Flush();
            streamMock.Verify(m => m.Flush(), Times.Once());

            streamMock.Setup(x => x.SetLength(123456));
            wrapper.SetLength(123456);
            streamMock.Verify(m => m.SetLength(123456), Times.Once());

            streamMock.Setup(x => x.Seek(1, SeekOrigin.Begin));
            wrapper.Seek(1, SeekOrigin.Begin);
            streamMock.Verify(m => m.Seek(1, SeekOrigin.Begin), Times.Once());
        }
    }
}
