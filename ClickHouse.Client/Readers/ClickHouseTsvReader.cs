using System;
using System.IO;
using System.Net.Http;

namespace ClickHouse.Client
{
    internal class ClickHouseTsvReader : ClickHouseDataReader
    {
        private readonly TextReader inputReader;

        public ClickHouseTsvReader(HttpResponseMessage httpResponse) : base(httpResponse)
        {
            inputReader = new StreamReader(InputStream);
            ReadHeaders();
        }

        public override bool Read()
        {
            if (!HasRows)
                return false;
            var rowItems = inputReader.ReadLine().Split('\t');
            if (rowItems.Length != FieldCount)
                throw new InvalidOperationException($"Wrong number of items in row ({rowItems.Length}), expected {FieldCount}");

            var rowData = new object[FieldCount];
            for (int i = 0; i < FieldCount; i++)
            {
                rowData[i] = Convert.ChangeType(rowItems[i], FieldTypes[i]);
            }
            CurrentRow = rowData;
            return true;
        }

        private void ReadHeaders()
        {
            var names = inputReader.ReadLine().Split('\t');
            var types = inputReader.ReadLine().Split('\t');

            if (names.Length != types.Length)
                throw new InvalidOperationException($"Count mismatch between names ({names.Length}) and types ({types.Length})");
            var fieldCount = names.Length;

            for (int i = 0; i < fieldCount; i++)
                FieldOrdinals.Add(names[i], i);

            FieldTypes = new Type[fieldCount];
            for (int i = 0; i < fieldCount; i++)
                FieldTypes[i] = ClickHouseTypeConverter.FromClickHouseType(types[i]);
        }
    }
}