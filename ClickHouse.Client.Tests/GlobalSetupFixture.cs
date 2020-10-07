using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    [SetUpFixture]
    public class GlobalSetupFixture
    {
        private readonly ClickHouseConnection connection;

        public GlobalSetupFixture()
        {
            connection = TestUtilities.GetTestClickHouseConnection();
        }

        [OneTimeSetUp]
        public async Task Setup()
        {
            await connection.ExecuteStatementAsync($"DROP DATABASE IF EXISTS test");
            await connection.ExecuteStatementAsync($"CREATE DATABASE test");
        }

        [OneTimeTearDown]
        public async Task Teardown()
        {
            await Task.FromResult(0);
            //await connection.ExecuteStatementAsync($"DROP DATABASE test");
        }
    }
}
