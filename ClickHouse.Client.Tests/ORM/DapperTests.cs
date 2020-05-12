using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using NUnit.Framework;
using Dapper;
using System.Linq;
using System.Collections.Generic;

namespace ClickHouse.Client.Tests.ORM
{
    public class DapperTests
    {
        private readonly ClickHouseConnection connection = TestUtilities.GetTestClickHouseConnection(default);

        [Test]
        public async Task ShouldExecuteSimpleSelect()
        {
            string sql = "SELECT * FROM system.table_functions";

            var functions = (await connection.QueryAsync<string>(sql)).ToList();
            CollectionAssert.IsNotEmpty(functions);
            CollectionAssert.AllItemsAreNotNull(functions);
        }

        [Test]
        public async Task ShouldExecuteSelectWithParameter()
        {
            var parameters = new Dictionary<string, object> { { "name", "mysql" } };
            string sql = "SELECT * FROM system.table_functions WHERE name = {name:String}";

            var functions = (await connection.QueryAsync<string>(sql, parameters)).ToList();
            CollectionAssert.IsNotEmpty(functions);
            CollectionAssert.AllItemsAreNotNull(functions);
        }

        [Test]
        [Ignore("Requires Dapper support, see https://github.com/StackExchange/Dapper/pull/1462")]
        public async Task ShouldExecuteSelectWithParameters()
        {
            var parameters = new Dictionary<string, object> { { "names", new[] { "mysql", "odbc" } } };
            string sql = "SELECT * FROM system.table_functions WHERE has({names:Array(String)}, name)";

            var functions = (await connection.QueryAsync<string>(sql, parameters)).ToList();
            CollectionAssert.IsNotEmpty(functions);
            CollectionAssert.AllItemsAreNotNull(functions);
        }
    }
}
