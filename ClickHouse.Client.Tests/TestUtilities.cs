using System;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    public static class TestUtilities
    {
        /// <summary>
        /// Utility method to allow to redirect ClickHouse connections to different machine, in case of Windows development environment
        /// </summary>
        /// <param name="driver">Type of ClickHouse driver to use</param>
        /// <returns></returns>
        public static ClickHouseConnection GetTestClickHouseConnection(ClickHouseConnectionDriver driver, bool compression = false)
        {
            // Developer override for Windows machine
            var devConnectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION") ??
                throw new InvalidOperationException("Must set CLICKHOUSE_CONNECTION environment variable pointing at ClickHouse server");

            var builder = new ClickHouseConnectionStringBuilder() { ConnectionString = devConnectionString };
            builder.Driver = driver; // Override driver with requested one
            builder.Compression = compression;
            return new ClickHouseConnection(builder.ConnectionString);
        }

        public static object[] GetEnsureSingleRow(this DbDataReader reader)
        {
            Assert.IsTrue(reader.HasRows);
            Assert.IsTrue(reader.Read());

            var data = reader.GetFieldValues();

            Assert.IsFalse(reader.HasRows);
            Assert.IsFalse(reader.Read());

            return data;
        }

        public static Task ExecuteStatementAsync(this ClickHouseConnection connection, string sql)
        {
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            return command.ExecuteNonQueryAsync();
        }

        public static Task<DbDataReader> ExecuteReaderAsync(this ClickHouseConnection connection, string sql)
        {
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            return command.ExecuteReaderAsync();
        }

        public static Type[] GetFieldTypes(this DbDataReader reader) => Enumerable.Range(0, reader.FieldCount).Select(reader.GetFieldType).ToArray();

        public static string[] GetFieldNames(this DbDataReader reader) => Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToArray();

        public static object[] GetFieldValues(this DbDataReader reader) => Enumerable.Range(0, reader.FieldCount).Select(reader.GetValue).ToArray();

        public static void EnsureFieldCount(this DbDataReader reader, int expectedCount) => Assert.AreEqual(expectedCount, reader.FieldCount);
    }
}
