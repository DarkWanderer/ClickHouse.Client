using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    [Parallelizable]
    [TestFixture(ClickHouseConnectionDriver.Binary)]
    [TestFixture(ClickHouseConnectionDriver.JSON)]
    [TestFixture(ClickHouseConnectionDriver.TSV)]
    public class SqlInsertTests
    {
        private readonly ClickHouseConnectionDriver driver;

        public SqlInsertTests(ClickHouseConnectionDriver driver)
        {
            this.driver = driver;
        }

        [SetUp]
        public async Task FixtureSetup()
        {
            using var connection = TestUtilities.GetTestClickHouseConnection(driver);
            await connection.ExecuteStatementAsync("CREATE DATABASE IF NOT EXISTS test");
            
            await connection.ExecuteStatementAsync("DROP TABLE IF EXISTS test.t_nested");
            await connection.ExecuteStatementAsync("CREATE TABLE IF NOT EXISTS test.t_nested(nested_v Nested (int16_v Int16, uint32_v UInt32, dtime_v DateTime, string_v String)) ENGINE = Memory");

            await connection.ExecuteStatementAsync("DROP TABLE IF EXISTS test.t_string");
            await connection.ExecuteStatementAsync("CREATE TABLE IF NOT EXISTS test.t_string(string_v String, fixedstring3_v FixedString(3)) ENGINE = Memory");
        }

        public static IEnumerable<TestCaseData> GetInsertQueryTestCases()
        {
            yield return new TestCaseData("INSERT INTO test.t_string", new Dictionary<string, object>() {
                { "string_v", "Part1\tPart2\nPart3" },
                { "fixedstring3_v", "ASDF"}
            }).SetName("DifferentTypeParametersInsert");
        }

        [Test]
        [TestCaseSource(typeof(SqlInsertTests), nameof(GetInsertQueryTestCases))]
        public async Task ShouldExecuteParameterizedInsertQuery(string sql, IReadOnlyDictionary<string, object> parameters)
        {
            using var connection = TestUtilities.GetTestClickHouseConnection(driver);
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            foreach (var keyValuePair in parameters)
            {
                var parameter = command.CreateParameter();
                parameter.ParameterName = keyValuePair.Key;
                parameter.Value = keyValuePair.Value;
            }
            await command.ExecuteNonQueryAsync();
        }
    }
}
