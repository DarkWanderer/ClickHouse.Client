using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    public class RawResultReaderAsyncTests
    {
        private readonly ClickHouseConnection connection = TestUtilities.GetTestClickHouseConnection(default);

        [Test]
        public async Task ShouldReadRawResult()
        {
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT 1 FORMAT TSV";
            using var result = await command.ExecuteRawResultAsync(CancellationToken.None);
            using var stream = await result.ReadAsStreamAsync();
            using var reader = new StreamReader(stream);
            Assert.AreEqual("1", reader.ReadToEnd().Trim());
        }
    }
}
