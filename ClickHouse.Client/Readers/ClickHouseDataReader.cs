using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Client
{
    public abstract class ClickHouseDataReader : DbDataReader, IDisposable
    {
        protected readonly HttpResponseMessage ServerResponse;
        protected readonly Stream InputStream;

        protected ClickHouseDataReader(HttpResponseMessage httpResponse)
        {
            ServerResponse = httpResponse;
            InputStream = httpResponse.Content.ReadAsStreamAsync().GetAwaiter().GetResult();
        }

        /// <summary>
        /// Values of current reader row. Must be filled in Read implementation of derived class
        /// </summary>
        protected object[] CurrentRow;

        /// <summary>
        /// Types of fields in reader. Must be filled in ReadHeaders implementation of derived class
        /// </summary>
        protected Type[] FieldTypes;

        /// <summary>
        /// Names of fields in reader. Must be filled in ReadHeaders implementation of derived class
        /// </summary>
        protected string[] FieldNames;

        public override object this[int ordinal] => GetValue(ordinal);

        public override object this[string name] => this[GetOrdinal(name)];

        public override int Depth { get; }

        public override int FieldCount => FieldTypes.Length;

        public override bool IsClosed => !HasRows;

        public override int RecordsAffected { get; }

        public override int VisibleFieldCount => base.VisibleFieldCount;

        public override bool GetBoolean(int ordinal) => Convert.ToBoolean(GetValue(ordinal));

        public override byte GetByte(int ordinal) => Convert.ToByte(GetValue(ordinal));

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length) => throw new NotImplementedException();

        public override char GetChar(int ordinal) => Convert.ToChar(GetValue(ordinal));

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length) => throw new NotImplementedException();

        public override string GetDataTypeName(int ordinal) => throw new NotImplementedException();

        public override DateTime GetDateTime(int ordinal) => throw new NotImplementedException();

        public override decimal GetDecimal(int ordinal) => Convert.ToDecimal(GetValue(ordinal));

        public override double GetDouble(int ordinal) => Convert.ToDouble(GetValue(ordinal));

        public override IEnumerator GetEnumerator() => throw new NotImplementedException();

        public override Type GetFieldType(int ordinal) => FieldTypes[ordinal];

        public override float GetFloat(int ordinal) => Convert.ToSingle(GetValue(ordinal));

        public override Guid GetGuid(int ordinal) => throw new NotImplementedException();

        public override short GetInt16(int ordinal) => Convert.ToInt16(GetValue(ordinal));

        public override int GetInt32(int ordinal) => Convert.ToInt32(GetValue(ordinal));

        public override long GetInt64(int ordinal) => Convert.ToInt64(GetValue(ordinal));

        public override string GetName(int ordinal) => FieldNames[ordinal];

        public override int GetOrdinal(string name)
        {
            var index = Array.FindIndex(FieldNames, (fn) => fn == name);
            if (index == -1)
                throw new IndexOutOfRangeException();
            return index;
        }

        public override string GetString(int ordinal) => Convert.ToString(GetValue(ordinal));

        public override object GetValue(int ordinal) => CurrentRow[ordinal];

        public override int GetValues(object[] values)
        {
            if (CurrentRow == null)
                throw new InvalidOperationException("Cannot get values before 'Read'");
            CurrentRow.CopyTo(values, 0);
            return CurrentRow.Length;
        }

        public override bool IsDBNull(int ordinal) => throw new NotImplementedException();
        public override bool NextResult() => throw new NotSupportedException();
        public override void Close() => base.Close();
        public override Task CloseAsync() => base.CloseAsync();
        protected override void Dispose(bool disposing) => base.Dispose(disposing);
        public override ValueTask DisposeAsync() => base.DisposeAsync();
        protected override DbDataReader GetDbDataReader(int ordinal) => base.GetDbDataReader(ordinal);
        public override Task<T> GetFieldValueAsync<T>(int ordinal, CancellationToken cancellationToken) => base.GetFieldValueAsync<T>(ordinal, cancellationToken);
        public override T GetFieldValue<T>(int ordinal) => base.GetFieldValue<T>(ordinal);
        public override Type GetProviderSpecificFieldType(int ordinal) => base.GetProviderSpecificFieldType(ordinal);
        public override object GetProviderSpecificValue(int ordinal) => base.GetProviderSpecificValue(ordinal);
        public override int GetProviderSpecificValues(object[] values) => base.GetProviderSpecificValues(values);
        public override DataTable GetSchemaTable() => base.GetSchemaTable();
        public override Stream GetStream(int ordinal) => base.GetStream(ordinal);
        public override TextReader GetTextReader(int ordinal) => base.GetTextReader(ordinal);
        public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken) => base.IsDBNullAsync(ordinal, cancellationToken);
        public override Task<bool> NextResultAsync(CancellationToken cancellationToken) => base.NextResultAsync(cancellationToken);
        public override bool Read() => throw new NotImplementedException();
        public override Task<bool> ReadAsync(CancellationToken cancellationToken) => base.ReadAsync(cancellationToken);
    }
}