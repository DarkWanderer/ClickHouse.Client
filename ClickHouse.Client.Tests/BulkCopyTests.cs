using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Client.Copy;
using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    //[Ignore("INSERT support is WIP")]
    public class BulkCopyTests
    {
        private readonly ClickHouseConnectionDriver driver = ClickHouseConnectionDriver.Binary;

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
            yield return new TestCaseData("DateTime('Asia/Ashkhabad')", new DateTime(2020, 2, 20, 20, 20, 20, DateTimeKind.Utc));
            // yield return new TestCaseData("Nested(A UInt8, B String)", new[] { Tuple.Create(1, "AAA"), Tuple.Create(2, "BBB") });
        }

        [Test]
        [TestCaseSource(typeof(BulkCopyTests), nameof(GetInsertSingleValueTestCases))]
        public async Task ShouldExecuteSingleValueInsertViaBulkCopy(string clickHouseType, object insertedValue)
        {
            using var connection = TestUtilities.GetTestClickHouseConnection(driver);

            var targetTable = $"temp.b_{clickHouseType}";
            targetTable = targetTable
                .Replace("(", null)
                .Replace(")", null)
                .Replace(",", null)
                .Replace(" ", null)
                .Replace("'", null)
                .Replace("/", null);

            clickHouseType = clickHouseType.Replace("Enum", "Enum('a' = 1, 'b' = 2)");

            await connection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {targetTable}");
            await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (value {clickHouseType}) ENGINE Memory");

            using var bulkCopy = new ClickHouseBulkCopy(connection)
            {
                DestinationTableName = targetTable,
            };

            await bulkCopy.WriteToServerAsync(Enumerable.Repeat(new[] { insertedValue }, 1));

            using var reader = await connection.ExecuteReaderAsync($"SELECT * from {targetTable}");
            Assert.IsTrue(reader.Read());
            reader.AssertHasFieldCount(1);
            var data = reader.GetValue(0);
            Assert.AreEqual(insertedValue, data);
        }
    }
}
