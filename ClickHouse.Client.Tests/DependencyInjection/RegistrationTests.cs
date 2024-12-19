using ClickHouse.Client.ADO;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;

namespace ClickHouse.Client.Tests.DependencyInjection;

public class RegistrationTests
{
    [Test]
    public void CanAddClickHouseDataSource()
    {
        const string connectionString = "Host=localhost;Port=1234";
        using var services = new ServiceCollection()
                             .AddClickHouseDataSource(connectionString)
                             .BuildServiceProvider();
        var dataSource = services.GetRequiredService<IClickHouseDataSource>();
        Assert.AreEqual(connectionString, dataSource.ConnectionString);

        using var fromService = services.GetRequiredService<IClickHouseConnection>();
        using var rawConnection = new ClickHouseConnection(connectionString);
        Assert.AreEqual(rawConnection.ConnectionString, fromService.ConnectionString);
    }
}
