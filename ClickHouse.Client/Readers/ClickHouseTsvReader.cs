using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using ClickHouse.Client.Types;

namespace ClickHouse.Client.Readers
{
    internal class ClickHouseTsvReader : ClickHouseDataReader
    {
        private readonly TextReader inputReader;

        public ClickHouseTsvReader(HttpResponseMessage httpResponse) : base(httpResponse)
        {
            inputReader = new StreamReader(InputStream);
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
                rowData[i] = ConvertString(rowItems[i], typeInfo);
            }
            CurrentRow = rowData;
            return true;
        }

        private object ConvertString(string item, TypeInfo typeInfo)
        {
            switch (typeInfo)
            {
                case ArrayTypeInfo ati:
                    return item
                       .Trim('[', ']')
                       .Split(',')
                       .Select(v => ConvertString(v, ati.UnderlyingType))
                       .ToArray();
                case NothingTypeInfo ti:
                    return item == "\\N" ? DBNull.Value : throw new InvalidOperationException();
                case NullableTypeInfo nti:
                    return item == "NULL" ? DBNull.Value : ConvertString(item, nti.UnderlyingType);
                default:
                    return Convert.ChangeType(item, typeInfo.EquivalentType, CultureInfo.InvariantCulture);
            };
        }

        private void ReadHeaders()
        {
            var names = inputReader.ReadLine().Split('\t');
            var types = inputReader.ReadLine().Split('\t');

            if (names.Length != types.Length)
                throw new InvalidOperationException($"Count mismatch between names ({names.Length}) and types ({types.Length})");
            var fieldCount = names.Length;
            RawTypes = new TypeInfo[fieldCount];
            FieldNames = new string[fieldCount];

            names.CopyTo(FieldNames, 0);
            for (var i = 0; i < fieldCount; i++)
                RawTypes[i] = TypeConverter.ParseClickHouseType(types[i]);
        }
    }
}