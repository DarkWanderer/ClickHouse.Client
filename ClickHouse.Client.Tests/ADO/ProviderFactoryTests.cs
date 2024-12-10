using System.Data.Common;
using ClickHouse.Client.ADO;
using ClickHouse.Client.ADO.Adapters;
using ClickHouse.Client.ADO.Parameters;
using NUnit.Framework;

namespace ClickHouse.Client.Tests.ADO;

public class ProviderFactoryTests
{
    [Test]
    public void ShouldProduceCorrectTypes()
    {
        DbProviderFactory factory = new ClickHouseConnectionFactory();
        Assert.IsInstanceOf<ClickHouseConnection>(factory.CreateConnection());
        Assert.IsInstanceOf<ClickHouseCommand>(factory.CreateCommand());
        Assert.IsInstanceOf<ClickHouseDataAdapter>(factory.CreateDataAdapter());
        Assert.IsInstanceOf<ClickHouseConnectionStringBuilder>(factory.CreateConnectionStringBuilder());
        Assert.IsInstanceOf<ClickHouseDbParameter>(factory.CreateParameter());
#if NET7_0_OR_GREATER
        Assert.IsInstanceOf<ClickHouseDataSource>(factory.CreateDataSource("Host=ignored"));
#endif

        // TODO
        // Assert.IsInstanceOf<ClickHouseConnectionStringBuilder>(factory.CreateCommandBuilder());
    }
}
