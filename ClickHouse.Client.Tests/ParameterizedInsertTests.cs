using System.Threading.Tasks;
using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    public class ParameterizedInsertTests : AbstractConnectionTestFixture
    {
        [Test]
        public async Task ShouldInsertParameterizedArray()
        {
            await connection.ExecuteStatementAsync("TRUNCATE TABLE IF EXISTS test.float_array");
            await connection.ExecuteStatementAsync("CREATE TABLE IF NOT EXISTS test.float_array (arr Array(Float64)) ENGINE Memory");

            var command = connection.CreateCommand();
            command.AddParameter("values", new[] { 1.0, 2.0, 3.0 });
            command.CommandText = "INSERT INTO test.float_array VALUES ({values:Array(Float32)})";
            await command.ExecuteNonQueryAsync();

            var count = await connection.ExecuteScalarAsync("SELECT COUNT(*) FROM test.float_array");
            Assert.AreEqual(1, count);
        }
    }
}
