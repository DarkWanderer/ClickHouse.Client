using System.Net.Http;
using System.Threading.Tasks;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    public class BasicConnectionTests
    {
        [Test]
        public void ShouldThrowHttpExceptionOnInvalidPort()
        {
            var connectionString = "Host=localhost,Port=44444";
            using var connection = new ClickHouseConnection(connectionString);
            Assert.Throws<HttpRequestException>(() => connection.Open());
        }

        [Test]
        public void ShouldConnectToExistingServer()
        {
            using var connection = new ClickHouseConnection();
            connection.Open();
        }

        [Test]
        public void ShouldDetermineServerVersion()
        {
            using var connection = new ClickHouseConnection();
            connection.Open();
        }
    }
}