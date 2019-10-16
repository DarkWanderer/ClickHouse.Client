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
            var devConnectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION") ?? "Host=localhost;Port=8123";

            var builder = new ClickHouseConnectionStringBuilder() { ConnectionString = devConnectionString };
            builder.Driver = driver; // Override driver with requested one
            return new ClickHouseConnection(builder.ConnectionString);
        }
    }
}
