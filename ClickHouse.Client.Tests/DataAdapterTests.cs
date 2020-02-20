using System.Data;
using System.Data.Common;
using ClickHouse.Client.ADO.Adapters;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    public class DataAdapterTests
    {
        private readonly DbConnection connection = TestUtilities.GetTestClickHouseConnection(ClickHouseConnectionDriver.Binary);

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
            using var connection = TestUtilities.GetTestClickHouseConnection(ClickHouseConnectionDriver.Binary);
            using var adapter = new ClickHouseDataAdapter();
            using var command = connection.CreateCommand();

            command.CommandText = "SELECT number, 'a' as string FROM system.numbers LIMIT 100";
            adapter.SelectCommand = command;

            var dataTable = new DataTable();
            adapter.Fill(dataTable);

            Assert.AreEqual(100, dataTable.Rows.Count);
            Assert.AreEqual(2, dataTable.Columns.Count);
        }
    }
}
