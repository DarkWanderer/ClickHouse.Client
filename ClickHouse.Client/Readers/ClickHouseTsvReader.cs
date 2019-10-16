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
        }

        public override bool Read() => throw new NotImplementedException();

        protected override void ReadHeaders()
        {
            var names = inputReader.ReadLine().Split('\t');
            var types = inputReader.ReadLine().Split('\t');

            for (int i = 0; i < names.Length; i++)
                FieldOrdinals.Add(names[i], i);

            FieldTypes = new Type[types.Length];
            for (int i = 0; i < types.Length; i++)
                FieldTypes[i] = ClickHouseTypeConverter.FromClickHouseType(types[i]);
        }
    }
}