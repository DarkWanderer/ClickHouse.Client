using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    public abstract class SqlCommandsTestSuiteBase
    {
        protected abstract ClickHouseConnectionDriver Driver { get; }

        [Test(Description = "Trivial 'SELECT 1' query")]
        public async Task ShouldExecuteSelect1()
        {
            using var connection = TestUtilities.GetTestClickHouseConnection(Driver);
            var command = connection.CreateCommand();
            command.CommandText = "SELECT 1";
            Assert.AreEqual(1, await command.ExecuteScalarAsync());
        }

        [Test(Description = "Trivial 'SELECT from numbers' query")]
        public void ShouldExecuteSelectRange()
        {
            const int count = 100;
            using var connection = TestUtilities.GetTestClickHouseConnection(Driver);
            var command = connection.CreateCommand();
            command.CommandText = $"SELECT number FROM system.numbers LIMIT {count}";
            using var reader = command.ExecuteReader();

            var results = new List<int>();

            Assert.IsTrue(reader.HasRows);
            Assert.AreEqual(1, reader.FieldCount);
            Assert.AreEqual(typeof(ulong), reader.GetFieldType(0));

            while (reader.HasRows)
            {
                reader.Read();
                results.Add(reader.GetInt32(0)); // Intentional conversion to int32
            }

            CollectionAssert.AreEqual(Enumerable.Range(0, count), results);
        }

        [Test(Description = "Cancel running query")]
        public async Task ShouldCancelRunningAsyncQuery()
        {
            using var connection = TestUtilities.GetTestClickHouseConnection(Driver);
            var command = connection.CreateCommand();
            command.CommandText = "SELECT number FROM system.numbers LIMIT 100000000";
            var task = command.ExecuteScalarAsync();
            command.Cancel();

            try
            {
                await task;
            }
            catch (TaskCanceledException)
            {
                // Correct
            }
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
