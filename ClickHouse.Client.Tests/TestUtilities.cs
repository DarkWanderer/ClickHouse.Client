using System;
using System.Collections.Generic;
using System.Text;

namespace ClickHouse.Client.Tests
{
    public static class TestUtilities
    {
        public static ClickHouseConnection GetTestClickHouseConnection(ClickHouseConnectionDriver driver)
        {
            var connectionString = $"Host=localhost;Port=8123;Driver={driver.ToString()}";
            return new ClickHouseConnection(connectionString);
        }
    }
}
