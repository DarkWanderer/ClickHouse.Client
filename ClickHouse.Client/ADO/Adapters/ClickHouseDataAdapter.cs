using System;
using System.Data;
using System.Data.Common;
using ClickHouse.Client.ADO.Readers;
using ClickHouse.Client.Types;

namespace ClickHouse.Client.ADO.Adapters
{
    public class ClickHouseDataAdapter : DbDataAdapter
    {
        public override int UpdateBatchSize { get => base.UpdateBatchSize; set => base.UpdateBatchSize = value; }

        protected override bool CanRaiseEvents => base.CanRaiseEvents;

        public override int Fill(DataSet dataSet) => base.Fill(dataSet);

        public override DataTable[] FillSchema(DataSet dataSet, SchemaType schemaType) => base.FillSchema(dataSet, schemaType);

        public override IDataParameter[] GetFillParameters() => base.GetFillParameters();

        public override bool ShouldSerializeAcceptChangesDuringFill() => base.ShouldSerializeAcceptChangesDuringFill();

        public override bool ShouldSerializeFillLoadOption() => base.ShouldSerializeFillLoadOption();

        public override int Update(DataSet dataSet) => base.Update(dataSet);

        protected override int AddToBatch(IDbCommand command) => base.AddToBatch(command);

        protected override void ClearBatch() => base.ClearBatch();

        protected override RowUpdatedEventArgs CreateRowUpdatedEvent(DataRow dataRow, IDbCommand command, StatementType statementType, DataTableMapping tableMapping) => base.CreateRowUpdatedEvent(dataRow, command, statementType, tableMapping);

        protected override RowUpdatingEventArgs CreateRowUpdatingEvent(DataRow dataRow, IDbCommand command, StatementType statementType, DataTableMapping tableMapping) => base.CreateRowUpdatingEvent(dataRow, command, statementType, tableMapping);

        protected override DataTableMappingCollection CreateTableMappings() => base.CreateTableMappings();

        protected override void Dispose(bool disposing) => base.Dispose(disposing);

        protected override int ExecuteBatch() => base.ExecuteBatch();

        protected override int Fill(DataSet dataSet, string srcTable, IDataReader dataReader, int startRecord, int maxRecords) => base.Fill(dataSet, srcTable, dataReader, startRecord, maxRecords);

        protected override int Fill(DataTable dataTable, IDataReader dataReader) => base.Fill(dataTable, dataReader);

        protected override int Fill(DataTable[] dataTables, IDataReader dataReader, int startRecord, int maxRecords)
        {
            if (dataTables is null)
                throw new ArgumentNullException(nameof(dataTables));
            if (dataTables.Length != 1)
                throw new ArgumentOutOfRangeException("Only single table fill is supported");
            if (maxRecords <= 0)
                maxRecords = int.MaxValue;

            var chReader = (ClickHouseDataReader)dataReader;
            var dataTable = dataTables[0];

            // Ensure columns are available in DataTable
            var columns = dataTable.Columns;
            for (int i = 0; i < dataReader.FieldCount; i++)
            {
                var name = dataReader.GetName(i);
                var existingColumn = columns[name];
                if (existingColumn == null)
                {
                    var chType = chReader.GetClickHouseType(i);
                    if (chType is NullableType nt)
                        chType = nt.UnderlyingType;
                    columns.Add(name, chType.FrameworkType);
                }
            }

            // Fill the datatable
            int rowNumber = 0;

            while (chReader.Read())
            {
                if (rowNumber < startRecord)
                    continue;
                if (rowNumber - startRecord > maxRecords)
                    break;

                var row = dataTable.NewRow();
                for (int i = 0; i < chReader.FieldCount; i++)
                {
                    row[chReader.GetName(i)] = chReader.GetValue(i);
                }
                dataTable.Rows.Add(row);
                rowNumber++;
            }

            return rowNumber;
        }

        protected override int Fill(DataSet dataSet, int startRecord, int maxRecords, string srcTable, IDbCommand command, CommandBehavior behavior) => base.Fill(dataSet, startRecord, maxRecords, srcTable, command, behavior);

        protected override int Fill(DataTable dataTable, IDbCommand command, CommandBehavior behavior) => base.Fill(dataTable, command, behavior);

        protected override int Fill(DataTable[] dataTables, int startRecord, int maxRecords, IDbCommand command, CommandBehavior behavior) => base.Fill(dataTables, startRecord, maxRecords, command, behavior);

        protected override DataTable[] FillSchema(DataSet dataSet, SchemaType schemaType, string srcTable, IDataReader dataReader) => base.FillSchema(dataSet, schemaType, srcTable, dataReader);

        protected override DataTable FillSchema(DataTable dataTable, SchemaType schemaType, IDataReader dataReader) => base.FillSchema(dataTable, schemaType, dataReader);

        protected override DataTable[] FillSchema(DataSet dataSet, SchemaType schemaType, IDbCommand command, string srcTable, CommandBehavior behavior) => base.FillSchema(dataSet, schemaType, command, srcTable, behavior);

        protected override DataTable FillSchema(DataTable dataTable, SchemaType schemaType, IDbCommand command, CommandBehavior behavior) => base.FillSchema(dataTable, schemaType, command, behavior);

        protected override IDataParameter GetBatchedParameter(int commandIdentifier, int parameterIndex) => base.GetBatchedParameter(commandIdentifier, parameterIndex);

        protected override bool GetBatchedRecordsAffected(int commandIdentifier, out int recordsAffected, out Exception error) => base.GetBatchedRecordsAffected(commandIdentifier, out recordsAffected, out error);

        protected override void InitializeBatching() => base.InitializeBatching();

        protected override void OnFillError(FillErrorEventArgs value) => base.OnFillError(value);

        protected override void OnRowUpdated(RowUpdatedEventArgs value) => base.OnRowUpdated(value);

        protected override void OnRowUpdating(RowUpdatingEventArgs value) => base.OnRowUpdating(value);

        protected override bool ShouldSerializeTableMappings() => base.ShouldSerializeTableMappings();

        protected override void TerminateBatching() => base.TerminateBatching();

        protected override int Update(DataRow[] dataRows, DataTableMapping tableMapping) => base.Update(dataRows, tableMapping);
    }
}
