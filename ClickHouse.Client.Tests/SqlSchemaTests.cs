using System.Data.Common;
using System.Threading.Tasks;
using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    public class SqlSchemaTests : AbstractConnectionTestFixture
    {
        [Test]
        public async Task ShouldGetReaderColumnSchema()
        {
            using var reader = await connection.ExecuteReaderAsync("SELECT 1 as num, 'a' as str");
            var schema = reader.GetColumnSchema();
            Assert.AreEqual(2, schema.Count);
            Assert.AreEqual("num", schema[0].ColumnName);
            Assert.AreEqual("str", schema[1].ColumnName);
        }

        [Test]
        public async Task ShouldGetReaderSchemaTable()
        {
            using var reader = await connection.ExecuteReaderAsync("SELECT 1 as num, 'a' as str");
            var schema = reader.GetSchemaTable();
            Assert.AreEqual(2, schema.Rows.Count);
        }
    }
}
