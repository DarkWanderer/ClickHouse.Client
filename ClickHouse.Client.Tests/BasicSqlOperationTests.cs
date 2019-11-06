using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Types;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    [Parallelizable]
    [TestFixture(ClickHouseConnectionDriver.Binary)]
    [TestFixture(ClickHouseConnectionDriver.JSON)]
    [TestFixture(ClickHouseConnectionDriver.TSV)]
    public class BasicSqlOperationTests
    {
        private readonly ClickHouseConnectionDriver driver;

        public BasicSqlOperationTests(ClickHouseConnectionDriver driver)
        {
            this.driver = driver;
        }

        public static IEnumerable<TestCaseData> GetSimpleQueryTestCases()
        {
            yield return new TestCaseData("SELECT NULL") { ExpectedResult = DBNull.Value };
            yield return new TestCaseData("SELECT 1") { ExpectedResult = 1 };
            yield return new TestCaseData("SELECT 1.5") { ExpectedResult = 1.5 };
            yield return new TestCaseData("SELECT 1e30") { ExpectedResult = 1e30 };
            yield return new TestCaseData("SELECT 'ASD'") { ExpectedResult = "ASD" };
            yield return new TestCaseData("SELECT toFixedString('ASD',3)") { ExpectedResult = "ASD" };
            yield return new TestCaseData("SELECT array(1, 2, 3)") { ExpectedResult = new[] { 1, 2, 3 } };

            yield return new TestCaseData("SELECT toDecimal32(123.45, 3)") { ExpectedResult = new decimal(123.45) };
            yield return new TestCaseData("SELECT toDecimal64(1.2345, 7)") { ExpectedResult = new decimal(1.2345) };
            yield return new TestCaseData("SELECT toDecimal128(12.34, 9)") { ExpectedResult = new decimal(12.34) };
            yield return new TestCaseData("SELECT tuple(1, 'a', NULL)") { ExpectedResult = new object[] { 1, "a", DBNull.Value } };

            yield return new TestCaseData("SELECT toDateOrNull('1988-11-12')") { ExpectedResult = new DateTime(1988, 11, 12) };
            yield return new TestCaseData("SELECT toDateTimeOrNull('1988-11-12 11:22:33')") { ExpectedResult = new DateTime(1988, 11, 12, 11, 22, 33) };
            yield return new TestCaseData("SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')") { ExpectedResult = new Guid("61f0c404-5cb3-11e7-907b-a6006ad3dba0") };
        }

        [Test]
        [TestCaseSource(typeof(BasicSqlOperationTests), nameof(GetSimpleQueryTestCases))]
        public async Task<object> ShouldSelectSingleValue(string sql)
        {
            using var connection = TestUtilities.GetTestClickHouseConnection(driver);
            var command = connection.CreateCommand();
            command.CommandText = sql;
            using var reader = await command.ExecuteReaderAsync();
            reader.EnsureFieldCount(1);
            return reader.GetEnsureSingleRow().Single();
        }

        [Test]
        public async Task ShouldSelectMultipleColumns()
        {
            using var connection = TestUtilities.GetTestClickHouseConnection(driver);
            var command = connection.CreateCommand();
            command.CommandText = "SELECT 1 as a, 2 as b, 3 as c";
            using var reader = await command.ExecuteReaderAsync();

            reader.EnsureFieldCount(3);
            reader.GetEnsureSingleRow();
            CollectionAssert.AreEqual(new[] { "a", "b", "c" }, reader.GetFieldNames());
            CollectionAssert.AreEqual(new[] { 1, 2, 3 }, reader.GetFieldValues());
        }

        [Test]
        public async Task ShouldSelectEmptyDataset()
        {
            using var connection = TestUtilities.GetTestClickHouseConnection(driver);
            var command = connection.CreateCommand();
            command.CommandText = "SELECT 1 LIMIT 0";
            using var reader = await command.ExecuteReaderAsync();

            reader.EnsureFieldCount(1);
            Assert.IsFalse(reader.HasRows);
            Assert.IsFalse(reader.Read());
        }

        [Test]
        public async Task ShouldSelectNumericTypes()
        {
            var types = Enum.GetValues(typeof(ClickHouseDataType))
                .Cast<ClickHouseDataType>()
                .Select(dt => dt.ToString())
                .Where(dt => dt.Contains("Int") || dt.Contains("Float"))
                .Select(dt => $"to{dt.ToString()}(55)")
                .ToArray();
            var sql = $"select {string.Join(',', types)}";

            using var connection = TestUtilities.GetTestClickHouseConnection(driver);
            var command = connection.CreateCommand();
            command.CommandText = sql;

            using var reader = await command.ExecuteReaderAsync();
            Assert.AreEqual(types.Length, reader.FieldCount);

            var data = reader.GetEnsureSingleRow();
            Assert.AreEqual(Enumerable.Repeat("55", data.Length), data.Select(x => x.ToString()));
        }

        [Test]
        public async Task ShouldSelectSingleColumnRange()
        {
            const int count = 100;
            using var connection = TestUtilities.GetTestClickHouseConnection(driver);
            var command = connection.CreateCommand();
            command.CommandText = $"SELECT number FROM system.numbers LIMIT {count}";
            using var reader = await command.ExecuteReaderAsync();

            var results = new List<int>();

            Assert.IsTrue(reader.HasRows);
            reader.EnsureFieldCount(1);
            Assert.AreEqual(typeof(ulong), reader.GetFieldType(0));

            while (reader.Read())
                results.Add(reader.GetInt32(0)); // Intentional conversion to int32

            Assert.IsFalse(reader.HasRows);
            CollectionAssert.AreEqual(Enumerable.Range(0, count), results);
        }

        [Test]
        public async Task ShouldCancelRunningAsyncQuery()
        {
            using var connection = TestUtilities.GetTestClickHouseConnection(driver);
            var command = connection.CreateCommand();
            command.CommandText = "SELECT sleep(3)";
            var task = command.ExecuteScalarAsync();
            await Task.Delay(100);
            command.Cancel();

            try
            {
                await task;
                Assert.Fail("Expected to receive TaskCancelledException from task");
            }
            catch (TaskCanceledException)
            {
                // Correct
            }
        }
    }
}
