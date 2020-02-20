using System.Data.Common;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    public class SessionConnectionTest
    {
        private DbConnection CreateConnection(bool useSession)
        {
            var builder = TestUtilities.GetConnectionStringBuilder();
            builder.UseSession = useSession;
            builder.Driver = ClickHouseConnectionDriver.Binary;
            builder.Compression = true;
            return new ClickHouseConnection(builder.ToString());
        }

        [Test]
        public async Task TempTableShouldBeCreatedSuccessfullyIfSessionEnabled()
        {
            using var connection = CreateConnection(true);
            await connection.ExecuteStatementAsync("CREATE TEMPORARY TABLE test_temp_table (value UInt8)");
            await connection.ExecuteScalarAsync("SELECT COUNT(*) from test_temp_table");
        }

        [Test]
        public async Task TempTableShouldFailIfSessionDisabled()
        {
            using var connection = CreateConnection(false);
            try
            {
                await connection.ExecuteStatementAsync("CREATE TEMPORARY TABLE test_temp_table (value UInt8)");
                Assert.Fail("ClickHouse should not be able to create temp table if session is disabled");
            } 
            catch (ClickHouseServerException e) when (e.ErrorCode == 113)
            {
            }
        }
    }
}
