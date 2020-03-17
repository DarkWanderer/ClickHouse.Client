using System;
using System.Data;
using System.Data.Common;
using ClickHouse.Client.ADO.Adapters;
using ClickHouse.Client.ADO.Readers;
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


        [Test]
        public void TestNullableWithoutAdaptorFillAndDataTableLoad()
        {

            using var command = connection.CreateCommand();

            command.CommandText =
            @"SELECT toHour(now()) AB,  toNullable(toFloat64(SUM(a)/1.434545)) AA,  toNullable(toFloat64(SUM(a)/1.4345445)) A1,  toNullable(toFloat64(SUM(a)/1.43234545)) A2
            from(
                SELECT  number as a from numbers(10000)
            )";

            var dataReader = (ClickHouseDataReader)command.ExecuteReader();

            var dataTable = new DataTable();


            for (var i = 0; i < dataReader.FieldCount; i++)
            {
                Type tr = dataReader.GetFieldType(i);
                if (Nullable.GetUnderlyingType(tr) != null)
                    tr = Nullable.GetUnderlyingType(tr);

                dataTable.Columns.Add
                (
                    new DataColumn
                    {
                        ColumnName = dataReader.GetName(i),
                        DataType = tr,
                        AllowDBNull = true
                    }
                );
            }

            while (dataReader.Read())
            {
                var row = dataTable.NewRow();
                for (int i = 0; i < dataReader.FieldCount; i++)
                {
                    row[i] = dataReader[i];
                }
                dataTable.Rows.Add(row);
            }
            Assert.AreEqual(1, dataTable.Rows.Count);

        }


        [Test]
        public void TestNullableWithDataTableLoad()
        {

            using var command = connection.CreateCommand();

            command.CommandText =
            @"SELECT toHour(now()) AB,  toNullable(toFloat64(SUM(a)/1.434545)) AA,  toNullable(toFloat64(SUM(a)/1.4345445)) A1,  toNullable(toFloat64(SUM(a)/1.43234545)) A2
            from(
                SELECT  number as a from numbers(10000)
            )";

            var dataReader = (ClickHouseDataReader)command.ExecuteReader();

            var dataTable = new DataTable();


            dataTable.Load(dataReader);

            Assert.AreEqual(1, dataTable.Rows.Count);

        }

        [Test]
        public void TestNullableWithFill()
        {


            using var adaptor = new ClickHouseDataAdapter();
            using var command = connection.CreateCommand();

            command.CommandText =
            @"SELECT toHour(now()) AB,  toNullable(toFloat64(SUM(a)/1.434545)) AA,  toNullable(toFloat64(SUM(a)/1.4345445)) A1,  toNullable(toFloat64(SUM(a)/1.43234545)) A2
            from(
                SELECT  number as a from numbers(10000)
            )";

            adaptor.SelectCommand = command;

            var dataTable = new DataTable();

            adaptor.Fill(dataTable);

            Assert.AreEqual(1, dataTable.Rows.Count);

            // dataTable.Load(dataReader);



        }
    }
}
