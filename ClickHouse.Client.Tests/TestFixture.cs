using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    [SetUpFixture]
    public class TestFixture
    {
        private readonly ClickHouseConnection connection;

        public TestFixture()
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
        public async Task Teardown() => await connection.ExecuteStatementAsync($"DROP DATABASE test");
    }
}
