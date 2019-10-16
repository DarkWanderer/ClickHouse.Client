using System;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading;

namespace ClickHouse.Client
{
    internal class ClickHouseCommand : DbCommand
    {
        private readonly ClickHouseConnection dbConnection;
        private readonly CancellationTokenSource cts = new CancellationTokenSource();

        public ClickHouseCommand(ClickHouseConnection connection)
        {
            dbConnection = connection;
        }

        public override string CommandText { get; set; }

        public override int CommandTimeout { get; set; }

        public override CommandType CommandType { get; set; }

        public override bool DesignTimeVisible { get; set; }

        public override UpdateRowSource UpdatedRowSource { get; set; }

        protected override DbConnection DbConnection
        {
            get => dbConnection;
            set => throw new NotSupportedException();
        }

        protected override DbParameterCollection DbParameterCollection { get; }

        protected override DbTransaction DbTransaction { get; set; }

        public override void Cancel() => cts.Cancel();

        public override int ExecuteNonQuery() => throw new NotImplementedException();

        public override object ExecuteScalar()
        {
            using var reader = ExecuteDbDataReader(CommandBehavior.Default);
            if (reader.HasRows)
            {
                reader.Read();
                return reader[0];
            }
            else
            {
                throw new InvalidOperationException("No data returned from query");
            }
        }

        public override void Prepare() => throw new NotImplementedException();

        protected override DbParameter CreateDbParameter() => throw new NotImplementedException();

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            var sqlBuilder = new StringBuilder(CommandText);
            var driver = dbConnection.Driver;
            switch (behavior)
            {
                case CommandBehavior.SingleRow:
                case CommandBehavior.SingleResult:
                    sqlBuilder.Append("\nLIMIT 1");
                    break;
                case CommandBehavior.SchemaOnly:
                    if (driver == ClickHouseConnectionDriver.JSON)
                        throw new NotSupportedException("JSON driver does not support fetching schema");
                    sqlBuilder.Append("\nLIMIT 0");
                    break;
                case CommandBehavior.CloseConnection:
                case CommandBehavior.Default:
                case CommandBehavior.KeyInfo:
                case CommandBehavior.SequentialAccess:
                    break;
            }
            switch (driver)
            {
                case ClickHouseConnectionDriver.Binary:
                    sqlBuilder.Append("\nFORMAT RowBinaryWithNamesAndTypes");
                    break;
                case ClickHouseConnectionDriver.JSON:
                    sqlBuilder.Append("\nFORMAT JSONEachRow");
                    break;
                case ClickHouseConnectionDriver.TSV:
                    sqlBuilder.Append("\nFORMAT TSVWithNamesAndTypes");
                    break;
            }

            var result = dbConnection.PostSqlQueryAsync(sqlBuilder.ToString()).GetAwaiter().GetResult();
            return driver switch
            {
                ClickHouseConnectionDriver.Binary => new ClickHouseBinaryReader(result),
                ClickHouseConnectionDriver.JSON => new ClickHouseJsonReader(result),
                ClickHouseConnectionDriver.TSV => new ClickHouseTsvReader(result),
                _ => throw new NotSupportedException("Unknown driver: " + driver.ToString()),
            };
        }
    }
}