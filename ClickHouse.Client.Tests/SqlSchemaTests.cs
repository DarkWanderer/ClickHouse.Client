using System.Data;
using System;
using System.Data.Common;
using System.Reflection.PortableExecutable;
using System.Threading.Tasks;
using ClickHouse.Client.Utility;
using NUnit.Framework;
using System.Linq;

namespace ClickHouse.Client.Tests;

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

    [Test]
    public void ShouldGetSchemaTableAsDataTable()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT name, total_rows from system.tables";
        using var reader = command.ExecuteReader();
        var table = new DataTable();
        try { 
            table.Load(reader);
        }
        catch
        {
            
        }
        var errors = table.GetErrors().Select(e => e.RowError).ToList();
        CollectionAssert.IsEmpty(errors);
    }
}
