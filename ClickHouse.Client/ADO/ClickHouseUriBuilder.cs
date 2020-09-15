using System;
using System.Collections.Concurrent;

namespace ClickHouse.Client.ADO
{
    internal class ClickHouseUriBuilder
    {
        private readonly ConcurrentDictionary<string, string> parameters = new ConcurrentDictionary<string, string>(); // Concurrent because of widely-available TryAdd

        public ClickHouseUriBuilder(Uri baseUri) => BaseUri = baseUri;

        public Uri BaseUri { get; }

        public bool AddQueryParameter(string name, string value) => parameters.TryAdd("param_" + name, value);

        public void AddQuerySetting(string name, string value) => parameters.TryAdd(name, value);

        public string Sql { get; set; }

        public bool UseCompression { get; set; }

        public string Database { get; set; }

        public string Session { get; set; }

        // TODO: move this method out of ClickHouseConnection
        public string MakeUri()
        {
            var uriBuilder = new UriBuilder(BaseUri);
            var queryParameters = new ClickHouseHttpQueryParameters()
            {
                Database = Database,
                UseHttpCompression = UseCompression,
                SqlQuery = Sql,
                SessionId = Session,
            };
            if (parameters != null)
            {
                foreach (var parameter in parameters)
                {
                    queryParameters.SetQueryParameter(parameter.Key, parameter.Value);
                }
            }

            uriBuilder.Query = queryParameters.ToString();
            return uriBuilder.ToString();
        }
    }
}
