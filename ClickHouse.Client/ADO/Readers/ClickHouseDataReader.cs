using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Client.Types;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.ADO.Readers
{
    public abstract class ClickHouseDataReader : DbDataReader
    {
        private readonly HttpResponseMessage httpResponse; // Used to dispose at the end of reader

        protected ClickHouseDataReader(HttpResponseMessage httpResponse)
        {
            this.httpResponse = httpResponse ?? throw new ArgumentNullException(nameof(httpResponse));
        }

        internal ClickHouseType GetClickHouseType(int ordinal) => RawTypes[ordinal];

        public override object this[int ordinal] => GetValue(ordinal);

        public override object this[string name] => this[GetOrdinal(name)];

        public override int Depth { get; }

        public override int FieldCount => RawTypes?.Length ?? throw new InvalidOperationException();

        public override bool IsClosed => false;

        public override sealed bool HasRows => true;

        public override int RecordsAffected { get; }

        public override int VisibleFieldCount => base.VisibleFieldCount;

        protected object[] CurrentRow { get; set; }

        protected string[] FieldNames { get; set; }

        private protected ClickHouseType[] RawTypes { get; set; }

        public override bool GetBoolean(int ordinal) => Convert.ToBoolean(GetValue(ordinal), CultureInfo.InvariantCulture);

        public override byte GetByte(int ordinal) => Convert.ToByte(GetValue(ordinal), CultureInfo.InvariantCulture);

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length) => throw new NotImplementedException();

        public override char GetChar(int ordinal) => Convert.ToChar(GetValue(ordinal), CultureInfo.InvariantCulture);

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length) => throw new NotImplementedException();

        public override string GetDataTypeName(int ordinal) => RawTypes[ordinal].ToString();

        public override DateTime GetDateTime(int ordinal) => (DateTime)GetValue(ordinal);

        public virtual DateTimeOffset GetDateTimeOffset(int ordinal)
        {
            var dt = GetDateTime(ordinal);
            return ((AbstractDateTimeType)RawTypes[ordinal]).ToDateTimeOffset(dt);
        }

        public override decimal GetDecimal(int ordinal) => Convert.ToDecimal(GetValue(ordinal), CultureInfo.InvariantCulture);

        public override double GetDouble(int ordinal) => Convert.ToDouble(GetValue(ordinal), CultureInfo.InvariantCulture);

        public override IEnumerator GetEnumerator() => CurrentRow.GetEnumerator();

        public override Type GetFieldType(int ordinal) => RawTypes[ordinal].FrameworkType;

        public override float GetFloat(int ordinal) => Convert.ToSingle(GetValue(ordinal), CultureInfo.InvariantCulture);

        public override Guid GetGuid(int ordinal)
        {
            var value = GetValue(ordinal);
            if (value is Guid guid)
            {
                return guid;
            }
            else if (value is string s)
            {
                return new Guid(s);
            }
            else
            {
                throw new InvalidOperationException($"Cannot convert value of type {value?.GetType()?.Name ?? "null"} to Guid");
            }
        }

        public override short GetInt16(int ordinal) => Convert.ToInt16(GetValue(ordinal), CultureInfo.InvariantCulture);

        public override int GetInt32(int ordinal) => Convert.ToInt32(GetValue(ordinal), CultureInfo.InvariantCulture);

        public override long GetInt64(int ordinal) => Convert.ToInt64(GetValue(ordinal), CultureInfo.InvariantCulture);

        public override string GetName(int ordinal) => FieldNames[ordinal];

        public override int GetOrdinal(string name)
        {
            var index = Array.FindIndex(FieldNames, (fn) => fn == name);
            if (index == -1)
            {
                throw new IndexOutOfRangeException();
            }

            return index;
        }

        public override string GetString(int ordinal) => Convert.ToString(GetValue(ordinal), CultureInfo.InvariantCulture);

        public override object GetValue(int ordinal) => CurrentRow[ordinal];

        public override int GetValues(object[] values)
        {
            if (CurrentRow == null)
            {
                throw new InvalidOperationException();
            }

            CurrentRow.CopyTo(values, 0);
            return CurrentRow.Length;
        }

        public override bool IsDBNull(int ordinal) => GetValue(ordinal) is DBNull || GetValue(ordinal) is null;

        public override bool NextResult() => false;

        public override void Close() => Dispose();

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                httpResponse?.Dispose();
            }
        }

        protected override DbDataReader GetDbDataReader(int ordinal) => base.GetDbDataReader(ordinal);

        public override Task<T> GetFieldValueAsync<T>(int ordinal, CancellationToken cancellationToken) => base.GetFieldValueAsync<T>(ordinal, cancellationToken);

        public override T GetFieldValue<T>(int ordinal) => (T)GetValue(ordinal);

        public override Type GetProviderSpecificFieldType(int ordinal) => base.GetProviderSpecificFieldType(ordinal);

        public override object GetProviderSpecificValue(int ordinal) => base.GetProviderSpecificValue(ordinal);

        public override int GetProviderSpecificValues(object[] values) => base.GetProviderSpecificValues(values);

        public override DataTable GetSchemaTable() => SchemaDescriber.DescribeSchema(this);

        public override Stream GetStream(int ordinal) => base.GetStream(ordinal);

        public override TextReader GetTextReader(int ordinal) => base.GetTextReader(ordinal);

        public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken) => base.IsDBNullAsync(ordinal, cancellationToken);

        public override Task<bool> NextResultAsync(CancellationToken cancellationToken) => Task.FromResult(false);

        public abstract override bool Read();

        public override Task<bool> ReadAsync(CancellationToken cancellationToken) => base.ReadAsync(cancellationToken);
    }
}
