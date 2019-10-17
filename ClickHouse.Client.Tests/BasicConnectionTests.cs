using System.Net.Http;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    public class BasicConnectionTests
    {
        [Test]
        public void ShouldConnectToServer()
        {
            using var connection = TestUtilities.GetTestClickHouseConnection(ClickHouseConnectionDriver.JSON);
            connection.Open();
            Assert.Pass($"Server version: {connection.ServerVersion}");
        }
    }
}