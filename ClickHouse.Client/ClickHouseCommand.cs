using System;
using System.Data;
using System.Data.Common;

namespace ClickHouse.Client
{
    internal class ClickHouseCommand : DbCommand
    {
        private readonly ClickHouseConnection dbConnection;

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

        public override void Cancel() => throw new System.NotImplementedException();

        public override int ExecuteNonQuery() => throw new System.NotImplementedException();

        public override object ExecuteScalar() => throw new System.NotImplementedException();

        public override void Prepare() => throw new System.NotImplementedException();

        protected override DbParameter CreateDbParameter() => throw new System.NotImplementedException();

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => throw new System.NotImplementedException();
    }
}