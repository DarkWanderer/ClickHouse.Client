using ClickHouse.Client.ADO;
using Microsoft.Extensions.DependencyInjection;

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
        ClassicAssert.AreEqual(connectionString, dataSource.ConnectionString);

        using var fromService = services.GetRequiredService<IClickHouseConnection>();
        using var rawConnection = new ClickHouseConnection(connectionString);
        ClassicAssert.AreEqual(rawConnection.ConnectionString, fromService.ConnectionString);
    }
}
