using ClickHouse.Client.ADO;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;

namespace ClickHouse.Client.Tests.DependencyInjection;

public class RegistrationTests
{
#if NET7_0_OR_GREATER
    [Test]
    public void CanAddClickHouseDataSource()
    {
        const string connectionString = "Host=localhost;Port=1234";
        using var services = new ServiceCollection()
                             .AddClickHouseDataSource(connectionString)
                             .BuildServiceProvider();
        var dataSource = services.GetRequiredService<IClickHouseDataSource>();
        Assert.That(dataSource.ConnectionString, Is.EqualTo(connectionString));

        using var fromService = services.GetRequiredService<IClickHouseConnection>();
        using var rawConnection = new ClickHouseConnection(connectionString);
        Assert.That(fromService.ConnectionString, Is.EqualTo(rawConnection.ConnectionString));
    }
#endif
}
