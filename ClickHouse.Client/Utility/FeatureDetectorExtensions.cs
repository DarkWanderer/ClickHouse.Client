using System;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;

namespace ClickHouse.Client.Utility
{
    public static class FeatureDetectorExtensions
    {
        /*
         ClickHouse Release 19.11.3.11, 2019-07-18
         New Feature
            Added support for prepared statements. #5331 (Alexander) #5630 (alexey-milovidov)
         */
        private static readonly Version ServerVersionSupportsHttpParameters = new Version(19, 11, 3, 11);

        public static async Task<bool> SupportsHttpParameters(this ClickHouseConnection connection)
        {
            if (connection.State != ConnectionState.Open)
                await connection.OpenAsync();
            if (string.IsNullOrWhiteSpace(connection.ServerVersion))
                throw new InvalidOperationException("Connection does not define server version");
            return Version.Parse(connection.ServerVersion) >= ServerVersionSupportsHttpParameters;
        }
    }
}
