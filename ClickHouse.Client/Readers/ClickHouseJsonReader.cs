using System.Net.Http;

namespace ClickHouse.Client
{
    internal class ClickHouseJsonReader : ClickHouseDataReader
    {   public ClickHouseJsonReader(HttpResponseMessage httpResponse) : base(httpResponse) 
        {
        }

        public override bool Read() => throw new System.NotImplementedException();
    }
}