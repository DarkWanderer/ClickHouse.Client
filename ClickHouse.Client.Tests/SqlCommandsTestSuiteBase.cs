using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    public abstract class SqlCommandsTestSuiteBase
    {
        protected abstract ClickHouseConnectionDriver Driver { get; }

        [Test]
        public void ShouldExecuteSelect1()
        {
            using var connection = new ClickHouseConnection();
            var command = connection.CreateCommand();
            command.CommandText = "SELECT 1";
            Assert.AreEqual(1, command.ExecuteScalar());
        }

        [Test]
        public void ShouldExecuteSelectRange()
        {
            using var connection = TestUtilities.GetTestClickHouseConnection(Driver);
            var command = connection.CreateCommand();
            command.CommandText = "SELECT number FROM system.numbers LIMIT 10";
            using var reader = command.ExecuteReader();

            var results = new List<int>();
            Assert.IsTrue(reader.HasRows);
            Assert.AreEqual(1, reader.FieldCount);
            while (reader.HasRows)
                Assert.AreEqual(typeof(int), reader.GetFieldType(1));
                results.Add((int)reader.GetValue(1));
            CollectionAssert.AreEqual(Enumerable.Range(1, 10), results);
        }

        [Test]
        public async Task ShouldCancelRunningAsyncQuery()
        {
            using var connection = TestUtilities.GetTestClickHouseConnection(Driver);
            var command = connection.CreateCommand();
            command.CommandText = "sleep(5); SELECT 1";
            var task = command.ExecuteScalarAsync();
            command.Cancel();
            await task;
        }
    }

    public class JsonDriverSqlQueryTestSuite : SqlCommandsTestSuiteBase
    {
        protected override ClickHouseConnectionDriver Driver => ClickHouseConnectionDriver.JSON;
    }
    public class BinaryDriverSqlQueryTestSuite : SqlCommandsTestSuiteBase
    {
        protected override ClickHouseConnectionDriver Driver => ClickHouseConnectionDriver.Binary;
    }
    public class TsvDriverSqlQueryTestSuite : SqlCommandsTestSuiteBase
    {
        protected override ClickHouseConnectionDriver Driver => ClickHouseConnectionDriver.TSV;
    }
}
