#if NET7_0_OR_GREATER
using ClickHouse.Client.ADO;
using NUnit.Framework;

namespace ClickHouse.Client.Tests.ADO;

public class DataSourceTests
{
    [Test]
    public void CanCreateConnection()
    {
        var connectionString = new ClickHouseConnection("Host=localhost").ConnectionString;
        using var dataSource = new ClickHouseDataSource(connectionString);
        using var connection = dataSource.CreateConnection();
        Assert.AreEqual(connection.ConnectionString, connectionString);
    }
}
#endif
