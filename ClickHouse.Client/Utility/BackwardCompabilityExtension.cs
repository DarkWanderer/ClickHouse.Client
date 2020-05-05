using System.Threading.Tasks;
using ClickHouse.Client.ADO;

namespace ClickHouse.Client.Utility
{
    using System;
    using System.Linq;

    public static class BackwardCompabilityExtension
    {
        /*
         ClickHouse Release 19.11.3.11, 2019-07-18
         New Feature
            Added support for prepared statements. #5331 (Alexander) #5630 (alexey-milovidov)
         */
        private static int[] httpParametersNotSupportedVersion = { 19, 11, 3, 11 };
        
        public static async Task<bool> HttpParametersSupported(this ClickHouseConnection connection)
        {
            if (string.IsNullOrWhiteSpace(connection.ServerVersion))
                await connection.OpenAsync();
            return IsHigherOrEquals(ParseVersion(connection.ServerVersion), httpParametersNotSupportedVersion);
        }

        private static bool IsHigherOrEquals(int[] currentVersion, int[] targetVersion)
        {
            for (int i = 0; i < targetVersion.Length; i++)
            {
                if (currentVersion.Length <= i || currentVersion[i] < targetVersion[i])
                    return false;
                if (currentVersion[i] > targetVersion[i])
                    return true;
            }

            return true;
        }

        private static int[] ParseVersion(string version)
        {
            return version.Split('.').Select(int.Parse).ToArray();
        }
    }
}
