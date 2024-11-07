using System;
using System.Data;
using System.Linq;
using System.Text;
using ClickHouse.Client.ADO;
using ClickHouse.Client.ADO.Adapters;
using ClickHouse.Client.ADO.Readers;
using ClickHouse.Client.Types;

namespace ClickHouse.Client.Utility;

internal static class SchemaDescriber
{
    public static DataTable DescribeSchema(this ClickHouseDataReader reader)
    {
        var table = new DataTable();
        table.Columns.Add("ColumnName", typeof(string));
        table.Columns.Add("ColumnOrdinal", typeof(int));
        table.Columns.Add("ColumnSize", typeof(int));
        table.Columns.Add("NumericPrecision", typeof(int));
        table.Columns.Add("NumericScale", typeof(int));
        table.Columns.Add("IsUnique", typeof(bool));
        table.Columns.Add("IsKey", typeof(bool));
        table.Columns.Add("DataType", typeof(Type));
        table.Columns.Add("AllowDBNull", typeof(bool));
        table.Columns.Add("ProviderType", typeof(string));
        table.Columns.Add("IsAliased", typeof(bool));
        table.Columns.Add("IsExpression", typeof(bool));
        table.Columns.Add("IsIdentity", typeof(bool));
        table.Columns.Add("IsAutoIncrement", typeof(bool));
        table.Columns.Add("IsRowVersion", typeof(bool));
        table.Columns.Add("IsHidden", typeof(bool));
        table.Columns.Add("IsLong", typeof(bool));
        table.Columns.Add("IsReadOnly", typeof(bool));

        for (int ordinal = 0; ordinal < reader.FieldCount; ordinal++)
        {
            var chType = reader.GetClickHouseType(ordinal);

            var row = table.NewRow();
            row["ColumnName"] = reader.GetName(ordinal);
            row["ColumnOrdinal"] = ordinal;
            row["ColumnSize"] = -1;
            row["DataType"] = chType is NullableType nt ? nt.UnderlyingType.FrameworkType : chType.FrameworkType;
            row["ProviderType"] = chType;
            row["IsLong"] = chType is StringType;
            row["AllowDBNull"] = chType is NullableType;
            row["IsReadOnly"] = true;
            row["IsRowVersion"] = false;
            row["IsUnique"] = false;
            row["IsKey"] = false;
            row["IsAutoIncrement"] = false;

            if (chType is DecimalType dt)
            {
                row["ColumnSize"] = dt.Size;
                row["NumericPrecision"] = dt.Precision;
                row["NumericScale"] = dt.Scale;
            }
            table.Rows.Add(row);
        }
        return table;
    }

    public static DataTable DescribeSchema(this ClickHouseConnection connection, string type, string[] restrictions) => type switch
    {
        "Columns" => DescribeColumns(connection, restrictions),
        _ => throw new NotSupportedException(),
    };

    private static DataTable DescribeColumns(ClickHouseConnection connection, string[] restrictions)
    {
        var command = connection.CreateCommand();
        var query = new StringBuilder("SELECT database as Database, table as Table, name as Name, type as ProviderType, type as DataType FROM system.columns");
        var database = restrictions != null && restrictions.Length > 0 ? restrictions[0] : null;
        var table = restrictions != null && restrictions.Length > 1 ? restrictions[1] : null;

        if (database != null)
        {
            query.Append(" WHERE database={database:String}");
            command.AddParameter("database", "String", database);
        }

        if (table != null)
        {
            query.Append(" AND table={table:String}");
            command.AddParameter("table", "String", table);
        }

        command.CommandText = query.ToString();
        using var adapter = new ClickHouseDataAdapter();
        adapter.SelectCommand = command;
        var result = new DataTable();
        adapter.Fill(result);

        foreach (var row in result.Rows.Cast<DataRow>())
        {
            var clickHouseType = TypeConverter.ParseClickHouseType((string)row["ProviderType"], TypeSettings.Default);
            row["ProviderType"] = clickHouseType.ToString();
            // TODO: this should return actual framework type like other implementations do
            row["DataType"] = clickHouseType.FrameworkType.ToString().Replace("System.", string.Empty);
        }

        return result;
    }
}
