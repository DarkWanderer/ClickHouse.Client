using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    public class BasicSqlCommandTests
    {
        [Test]
        public void ShouldExecuteSelect1()
        {
            using var connection = new ClickHouseConnection();
            var command = connection.CreateCommand();
            command.CommandText = "SELECT 1";
            Assert.Equals(1, command.ExecuteScalar());
        }

        [Test]
        public void ShouldExecuteSelectRange()
        {
            using var connection = new ClickHouseConnection();
            var command = connection.CreateCommand();
            command.CommandText = "SELECT number FROM system.numbers LIMIT 10";
            using var reader = command.ExecuteReader();

            var results = new List<int>();
            Assert.IsTrue(reader.HasRows);
            Assert.Equals(1, reader.FieldCount);
            while (reader.HasRows)
                Assert.Equals(typeof(int), reader.GetFieldType(1));
                results.Add((int)reader.GetValue(1));
            CollectionAssert.AreEqual(Enumerable.Range(1, 10), results);
        }

        [Test]
        public async Task ShouldCancelRunningAsyncQuery()
        {
            using var connection = new ClickHouseConnection();
            var command = connection.CreateCommand();
            command.CommandText = "SELECT sleep(5)";
            var task = command.ExecuteScalarAsync();
            command.Cancel();
            await task;
        }
    }
}
