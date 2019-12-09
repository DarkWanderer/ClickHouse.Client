using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using ClickHouse.Client.Types;

namespace ClickHouse.Client.ADO.Readers
{
    internal class ClickHouseTsvReader : ClickHouseDataReader
    {
        private readonly TextReader inputReader;

        public ClickHouseTsvReader(HttpResponseMessage httpResponse) : base(httpResponse)
        {
            inputReader = new StreamReader(httpResponse.Content.ReadAsStreamAsync().GetAwaiter().GetResult());
            ReadHeaders();
        }

        public override bool HasRows => inputReader.Peek() != -1;

        public override bool Read()
        {
            if (!HasRows)
                return false;
            var rowItems = inputReader.ReadLine().Split('\t');
            if (rowItems.Length != FieldCount)
                throw new InvalidOperationException($"Wrong number of items in row ({rowItems.Length}), expected {FieldCount}");

            var rowData = new object[FieldCount];
            for (var i = 0; i < FieldCount; i++)
            {
                var typeInfo = RawTypes[i];
                if (typeInfo.DataType == ClickHouseTypeCode.UUID)
                    rowData[i] = new Guid(rowItems[i]);
                else
                    rowData[i] = ConvertString(rowItems[i], typeInfo);
            }
            CurrentRow = rowData;
            return true;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                inputReader.Dispose();
            }
        }

        private object ConvertString(string item, ClickHouseType typeInfo)
        {
            switch (typeInfo)
            {
                case ArrayType ati:
                    return item
                      .Trim('[', ']')
                      .Split(',')
                      .Select(v => ConvertString(v, ati.UnderlyingType))
                      .ToArray();
                case TupleType tti:
                    return ParseTuple(item, tti);
                case NothingType ti:
                    return item == "\\N" ? DBNull.Value : throw new InvalidOperationException();
                case NullableType nti:
                    return item == "NULL" ? DBNull.Value : ConvertString(item, nti.UnderlyingType);
                default:
                    return typeInfo.DataType == ClickHouseTypeCode.UUID
                        ? new Guid(item)
                        : Convert.ChangeType(item, typeInfo.EquivalentType, CultureInfo.InvariantCulture);
            };
        }

        private object[] ParseTuple(string item, TupleType tti)
        {
            var trimmed = item.Substring(1).Remove(item.Length - 2);
            var types = tti.UnderlyingTypes;
            var items = trimmed.Split(',');
            var result = new object[types.Length];
            for (var i = 0; i < types.Length; i++)
                result[i] = ConvertString(items[i].Trim('\''), types[i]);
            return result;
        }

        private void ReadHeaders()
        {
            var names = inputReader.ReadLine().Split('\t');
            var types = inputReader.ReadLine().Split('\t');

            if (names.Length != types.Length)
                throw new InvalidOperationException($"Count mismatch between names ({names.Length}) and types ({types.Length})");
            var fieldCount = names.Length;
            RawTypes = new ClickHouseType[fieldCount];
            FieldNames = new string[fieldCount];

            names.CopyTo(FieldNames, 0);
            for (var i = 0; i < fieldCount; i++)
                RawTypes[i] = TypeConverter.ParseClickHouseType(types[i]);
        }
    }
}