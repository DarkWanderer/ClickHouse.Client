using ClickHouse.Client.ADO;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    public class BasicConnectionTests
    {
        [Test]
        public void ShouldConnectToServer()
        {
            using var connection = TestUtilities.GetTestClickHouseConnection(default);
            connection.Open();
            Assert.Pass($"Server version: {connection.ServerVersion}");
            connection.Close();
        }
    }
}