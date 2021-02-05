using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    public class RawResultReaderAsyncTests : AbstractConnectionTestFixture
    {
        [Test]
        public async Task ShouldReadRawResultAsStream()
        {
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT 1,2,3 FORMAT TSV";
            using var result = await command.ExecuteRawResultAsync(CancellationToken.None);
            using var stream = await result.ReadAsStreamAsync();
            using var reader = new StreamReader(stream);
            Assert.AreEqual("1\t2\t3\n", reader.ReadToEnd());
        }

        [Test]
        public async Task ShouldCopyRawResultToStream()
        {
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT 1,2,3 FORMAT TSV";
            using var result = await command.ExecuteRawResultAsync(CancellationToken.None);
            using var stream = new MemoryStream();
            await result.CopyToAsync(stream);

            stream.Seek(0, SeekOrigin.Begin);

            using var reader = new StreamReader(stream);
            Assert.AreEqual("1\t2\t3\n", reader.ReadToEnd());
        }

        [Test]
        public async Task ShouldReadRawResultAsArray()
        {
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT 1,2,3 FORMAT TSV";
            using var result = await command.ExecuteRawResultAsync(CancellationToken.None);
            var array = await result.ReadAsByteArrayAsync();
            Assert.AreEqual(Encoding.UTF8.GetBytes("1\t2\t3\n"), array);
        }

        [Test]
        public async Task ShouldReadRawResultAsString()
        {
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT 1,2,3 FORMAT TSV";
            using var result = await command.ExecuteRawResultAsync(CancellationToken.None);
            var @string = await result.ReadAsStringAsync();
            Assert.AreEqual("1\t2\t3\n", @string);
        }
    }
}
