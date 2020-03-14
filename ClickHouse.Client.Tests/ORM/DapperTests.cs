using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using NUnit.Framework;
using Dapper;
using System.Linq;

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
        [Ignore("Parameters support is WIP")]
        public async Task ShouldExecuteSelectWithParameters ()
        {
            string sql = "SELECT * FROM system.table_functions WHERE name IN @Names";

            var functions = (await connection.QueryAsync<string>(sql, new { Names = new string[] { "mysql", "odbc" } } )).ToList();
            CollectionAssert.IsNotEmpty(functions);
            CollectionAssert.AllItemsAreNotNull(functions);
        }
    }
}
