using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using ClickHouse.Client.ADO.Adapters;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
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

            Assert.AreEqual(1, dataSet.Tables.Count);
            Assert.AreEqual(100, dataSet.Tables[0].Rows.Count);
            Assert.AreEqual(2, dataSet.Tables[0].Columns.Count);
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

            Assert.AreEqual(100, dataTable.Rows.Count);
            Assert.AreEqual(2, dataTable.Columns.Count);
        }

        public static IEnumerable<TestCaseData> SimpleSelectQueries => TestUtilities.GetDataTypeSamples()
            .Where(sample => sample.ExampleValue != DBNull.Value)
            .Select(sample => new TestCaseData($"SELECT {sample.ExampleExpression} AS col"));

        [Test]
        [Parallelizable]
        [TestCaseSource(typeof(DataAdapterTests), nameof(SimpleSelectQueries))]
        public void DataAdapterShouldFillSingleValue(string sql)
        {
            using var adapter = new ClickHouseDataAdapter();
            using var command = connection.CreateCommand();

            command.CommandText = sql;
            adapter.SelectCommand = command;

            var dataTable = new DataTable();
            adapter.Fill(dataTable);

            Assert.AreEqual(1, dataTable.Rows.Count);
            Assert.AreEqual(1, dataTable.Columns.Count);
            Assert.AreEqual("col", dataTable.Columns[0].ColumnName);
        }
    }
}
