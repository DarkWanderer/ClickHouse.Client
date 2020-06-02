using System.Data.Common;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    public class SqlAlterTests
    {
        private readonly DbConnection connection;

        public SqlAlterTests()
        {
            var builder = TestUtilities.GetConnectionStringBuilder();
            builder.UseSession = true;
            builder.Compression = true;
            connection = new ClickHouseConnection(builder.ToString());
        }

        [Test]
        public async Task ShouldExecuteAlterTable()
        {
            await connection.ExecuteStatementAsync("CREATE DATABASE IF NOT EXISTS test");
            await connection.ExecuteStatementAsync("CREATE TABLE IF NOT EXISTS test.table_delete_from (value Int32) ENGINE=MergeTree ORDER BY value");
            await connection.ExecuteStatementAsync("ALTER TABLE test.table_delete_from DELETE WHERE 1=1");
        }
    }
}
