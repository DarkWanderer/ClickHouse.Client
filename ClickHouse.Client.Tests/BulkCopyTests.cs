using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    //[Ignore("INSERT support is WIP")]
    public class BulkCopyTests
    {
        private readonly ClickHouseConnectionDriver driver = ClickHouseConnectionDriver.TSV;

        [SetUp]
        public async Task FixtureSetup()
        {
            using var connection = TestUtilities.GetTestClickHouseConnection(driver);
            await connection.ExecuteStatementAsync("CREATE DATABASE IF NOT EXISTS temp");
        }

        public static IEnumerable<TestCaseData> GetInsertSingleValueTestCases()
        {
            foreach (var sample in TestUtilities.GetDataTypeSamples().Where(s => s.ClickHouseType != "Nothing"))
            {
                yield return new TestCaseData(sample.ClickHouseType, sample.ExampleValue);
            }
            yield return new TestCaseData("String", "1\t2\n3");
        }

        [Test]
        [TestCaseSource(typeof(BulkCopyTests), nameof(GetInsertSingleValueTestCases))]
        public async Task ShouldExecuteSingleValueInsertViaBulkCopy(string clickHouseType, object insertedValue)
        {
            using var connection = TestUtilities.GetTestClickHouseConnection(driver);

            var targetTable = $"temp.b_{clickHouseType.Replace("(", null).Replace(")", null).Replace(",", null).Replace(" ", null) }";
            await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (value {clickHouseType}) ENGINE Memory");

            using var bulkCopy = new ClickHouseBulkCopy(connection)
            {
                DestinationTableName = targetTable,
                BatchSize = 100000
            };

            await bulkCopy.WriteToServerAsync(Enumerable.Repeat(new[] { insertedValue }, 1));

            using var reader = await connection.ExecuteReaderAsync($"SELECT * from {targetTable}");
            Assert.IsTrue(reader.Read());
            reader.EnsureFieldCount(1);
            var data = reader.GetValue(0);
            Assert.AreEqual(insertedValue, data);
        }
    }
}
