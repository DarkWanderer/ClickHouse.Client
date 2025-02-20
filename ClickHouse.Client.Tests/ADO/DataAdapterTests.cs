using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Client.ADO.Adapters;
using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests.ADO;

public class DataAdapterTests : AbstractConnectionTestFixture
{
    [Test]
    public void DataAdapterShouldFillDataSet()
    {

        using var adapter = new ClickHouseDataAdapter();
        using var command = connection.CreateCommand();

        command.CommandText = "SELECT number, 'a' as string FROM system.numbers LIMIT 100";
        adapter.SelectCommand = command;

        var dataSet = new DataSet();
        adapter.Fill(dataSet);

        Assert.That(dataSet.Tables.Count, Is.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(dataSet.Tables[0].Rows.Count, Is.EqualTo(100));
            Assert.That(dataSet.Tables[0].Columns.Count, Is.EqualTo(2));
        });
    }

    [Test]
    public void DataAdapterShouldFillDataTable()
    {
        using var connection = TestUtilities.GetTestClickHouseConnection();
        using var adapter = new ClickHouseDataAdapter();
        using var command = connection.CreateCommand();

        command.CommandText = "SELECT number, 'a' as string FROM system.numbers LIMIT 100";
        adapter.SelectCommand = command;

        var dataTable = new DataTable();
        adapter.Fill(dataTable);

        Assert.Multiple(() =>
        {
            Assert.That(dataTable.Rows.Count, Is.EqualTo(100));
            Assert.That(dataTable.Columns.Count, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task DataTableShouldLoadResults()
    {
        using var connection = TestUtilities.GetTestClickHouseConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 as SOMEID FROM numbers(10)";
        using var reader = await command.ExecuteReaderAsync();
        var table = new DataTable();
        table.Load(reader);
    }

    public static IEnumerable<TestCaseData> SimpleSelectQueries => TestUtilities.GetDataTypeSamples()
        .Where(sample => sample.ExampleValue != DBNull.Value)
        .Select(sample => new TestCaseData($"SELECT {sample.ExampleExpression} AS col"));

    [Test]
    [TestCaseSource(typeof(DataAdapterTests), nameof(SimpleSelectQueries))]
    public void DataAdapterShouldFillSingleValue(string sql)
    {
        using var adapter = new ClickHouseDataAdapter();
        using var command = connection.CreateCommand();

        command.CommandText = sql;
        adapter.SelectCommand = command;

        var table = new DataTable();
        adapter.Fill(table);

        Assert.Multiple(() =>
        {
            Assert.That(table.Rows.Count, Is.EqualTo(1));
            Assert.That(table.Columns.Count, Is.EqualTo(1));
        });
        Assert.That(table.Columns[0].ColumnName, Is.EqualTo("col"));
    }

    [Test]
    [TestCaseSource(typeof(DataAdapterTests), nameof(SimpleSelectQueries))]
    public void ShouldReadDataTable(string sql)
    {
        using var adapter = new ClickHouseDataAdapter();
        using var table = connection.ExecuteDataTable(sql);

        Assert.Multiple(() =>
        {
            Assert.That(table.Rows.Count, Is.EqualTo(1));
            Assert.That(table.Columns.Count, Is.EqualTo(1));
        });
        Assert.That(table.Columns[0].ColumnName, Is.EqualTo("col"));
    }
}
