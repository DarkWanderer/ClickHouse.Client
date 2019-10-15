using System.Net.Http;
using System.Threading.Tasks;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    public class BasicConnectionTest
    {
        [Test]
        public void ShouldThrowHttpExceptionOnInvalidPort()
        {
            var connectionString = "Host=localhost;Port=44444";
            using (var connection = new ClickHouseConnection(connectionString))
            {
                Assert.Throws<HttpRequestException>(() => connection.Open());
            }
        }
    }
}