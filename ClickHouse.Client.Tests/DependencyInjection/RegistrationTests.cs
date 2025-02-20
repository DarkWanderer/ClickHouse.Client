namespace ClickHouse.Client.Tests.DependencyInjection;

public class RegistrationTests
{
#if NET50_OR_GREATER
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
#endif
}
