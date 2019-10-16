using System.Net.Http;

namespace ClickHouse.Client
{
    internal class ClickHouseBinaryReader : ClickHouseDataReader
    {
        public ClickHouseBinaryReader(HttpResponseMessage httpResponse) : base(httpResponse) { }

        public override bool HasRows { get; }

        public override bool Read() => throw new System.NotImplementedException();
    }
}