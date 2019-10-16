using System.Net.Http;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    public class BasicConnectionTests
    {
        [Test]
        [Explicit]
        public void ShouldThrowHttpExceptionOnInvalidPort()
        {
            Assert.Inconclusive();
            var connectionString = "Host=localhost;Port=44444";
            using var connection = new ClickHouseConnection(connectionString);
            Assert.Throws<HttpRequestException>(() => connection.Open());
        }

        [Test]
        public void ShouldConnectToExistingServer()
        {
            using var connection = TestUtilities.GetTestClickHouseConnection(ClickHouseConnectionDriver.JSON);
            connection.Open();
            Assert.Pass($"Server version: {connection.ServerVersion}");
        }
    }
}