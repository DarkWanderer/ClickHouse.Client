using System;
using System.Collections.Generic;
using System.Text;

namespace ClickHouse.Client.Tests
{
    public static class TestUtilities
    {
        /// <summary>
        /// Utility method to allow to redirect ClickHouse connections to different machine, in case of Windows development environment
        /// </summary>
        /// <param name="driver">Type of ClickHouse driver to use</param>
        /// <returns></returns>
        public static ClickHouseConnection GetTestClickHouseConnection(ClickHouseConnectionDriver driver)
        {
            // Developer override for Windows machine
            var devConnectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION") ??
                (IsUnix ? "Host=localhost;Port=8123" : throw new InvalidOperationException("Must set CLICKHOUSE_CONNECTION variable on Windows"));

            var builder = new ClickHouseConnectionStringBuilder() { ConnectionString = devConnectionString };
            builder.Driver = driver; // Override driver with requested one
            return new ClickHouseConnection(builder.ConnectionString);
        }

        private static bool IsUnix => Environment.OSVersion.Platform == PlatformID.Unix;
    }
}
