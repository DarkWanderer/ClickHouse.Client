using System;
using System.Collections.Specialized;
using System.Globalization;
using System.Web;
using ClickHouse.Client.ADO.Parameters;
using ClickHouse.Client.Types;

namespace ClickHouse.Client.ADO
{
    internal class ClickHouseHttpQueryParameters : ICloneable
    {
        private readonly NameValueCollection parameterCollection;

        public ClickHouseHttpQueryParameters()
            : this(string.Empty) { }

        public ClickHouseHttpQueryParameters(string query)
        {
            parameterCollection = HttpUtility.ParseQueryString(query);
            // Do not put quotes around 64-bit integers
            parameterCollection.Set("output_format_json_quote_64bit_integers", false.ToString());
        }

        public string Database
        {
            get => parameterCollection.Get("database");
            set => parameterCollection.Set("database", value);
        }

        public bool UseHttpCompression
        {
            get => parameterCollection.Get("enable_http_compression").Equals("true", StringComparison.OrdinalIgnoreCase);
            set => parameterCollection.Set("enable_http_compression", value.ToString(CultureInfo.InvariantCulture));
        }

        public string SqlQuery
        {
            get => parameterCollection.Get("query");
            set => SetOrRemove("query", value);
        }

        public string SessionId
        {
            get => parameterCollection.Get("session_id");
            set => SetOrRemove("session_id", value);
        }

        public void SetParameter(string name, string value)
        {
            parameterCollection.Set($"param_{name}", value);
        }

        public object Clone() => new ClickHouseHttpQueryParameters(ToString());

        public override string ToString() => parameterCollection.ToString();

        private void SetOrRemove(string name, string value)
        {
            if (!string.IsNullOrEmpty(value))
            {
                parameterCollection.Set(name, value);
            }
            else
            {
                parameterCollection.Remove(name);
            }
        }
    }
}
