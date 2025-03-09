using System.Data;
using System.Data.Common;
using System.Threading.Tasks;
using ClickHouse.Client.Utility;
using NUnit.Framework;
using System.Linq;

namespace ClickHouse.Client.Tests.ADO;

public class SqlSchemaTests : AbstractConnectionTestFixture
{
    [Test]
    public async Task ShouldGetReaderColumnSchema()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT 1 as num, 'a' as str");
        var schema = reader.GetColumnSchema();
        Assert.That(schema.Count, Is.EqualTo(2));
        Assert.Multiple(() =>
        {
            Assert.That(schema[0].ColumnName, Is.EqualTo("num"));
            Assert.That(schema[1].ColumnName, Is.EqualTo("str"));
        });
    }

    [Test]
    public async Task ShouldGetReaderSchemaTable()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT 1 as num, 'a' as str");
        var schema = reader.GetSchemaTable();
        Assert.That(schema.Rows.Count, Is.EqualTo(2));
    }

    [Test]
    public void ShouldGetSchemaTableAsDataTable()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT name, total_rows from system.tables";
        using var reader = command.ExecuteReader();
        var table = new DataTable();
        try
        {
            table.Load(reader);
        }
        catch
        {

        }
        var errors = table.GetErrors().Select(e => e.RowError).ToList();
        Assert.That(errors, Is.Empty);
    }
}
