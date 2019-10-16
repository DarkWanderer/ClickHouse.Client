using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Net.Http;

namespace ClickHouse.Client
{
    public abstract class ClickHouseDataReader : DbDataReader, IDisposable
    {
        protected readonly HttpResponseMessage ServerResponse;
        protected readonly IDictionary<string, int> FieldOrdinals = new Dictionary<string, int>();
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

        public override object this[int ordinal] => GetValue(ordinal);

        public override object this[string name] => this[GetOrdinal(name)];

        public override int Depth { get; }

        public override int FieldCount => FieldTypes.Length;

        public override bool HasRows => InputStream.Position < InputStream.Length;

        public override bool IsClosed => !HasRows;

        public override int RecordsAffected { get; }

        public override bool GetBoolean(int ordinal) => throw new NotImplementedException();

        public override byte GetByte(int ordinal) => throw new NotImplementedException();

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length) => throw new NotImplementedException();

        public override char GetChar(int ordinal) => throw new NotImplementedException();

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length) => throw new NotImplementedException();

        public override string GetDataTypeName(int ordinal) => throw new NotImplementedException();

        public override DateTime GetDateTime(int ordinal) => throw new NotImplementedException();

        public override decimal GetDecimal(int ordinal) => throw new NotImplementedException();

        public override double GetDouble(int ordinal) => throw new NotImplementedException();

        public override IEnumerator GetEnumerator() => throw new NotImplementedException();

        public override Type GetFieldType(int ordinal) => FieldTypes[ordinal];

        public override float GetFloat(int ordinal) => throw new NotImplementedException();

        public override Guid GetGuid(int ordinal) => throw new NotImplementedException();

        public override short GetInt16(int ordinal) => throw new NotImplementedException();

        public override int GetInt32(int ordinal) => throw new NotImplementedException();

        public override long GetInt64(int ordinal) => throw new NotImplementedException();

        public override string GetName(int ordinal) => FieldOrdinals.Where(kvp => kvp.Value == ordinal).Select(kvp => kvp.Key).SingleOrDefault();

        public override int GetOrdinal(string name) => FieldOrdinals.TryGetValue(name, out int value) ? value : -1;

        public override string GetString(int ordinal) => throw new NotImplementedException();

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
    }
}