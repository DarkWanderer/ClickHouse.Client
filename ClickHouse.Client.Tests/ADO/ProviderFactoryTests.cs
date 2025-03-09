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
        ClassicAssert.IsInstanceOf<ClickHouseConnection>(factory.CreateConnection());
        ClassicAssert.IsInstanceOf<ClickHouseCommand>(factory.CreateCommand());
        ClassicAssert.IsInstanceOf<ClickHouseDataAdapter>(factory.CreateDataAdapter());
        ClassicAssert.IsInstanceOf<ClickHouseConnectionStringBuilder>(factory.CreateConnectionStringBuilder());
        ClassicAssert.IsInstanceOf<ClickHouseDbParameter>(factory.CreateParameter());
#if NET7_0_OR_GREATER
        ClassicAssert.IsInstanceOf<ClickHouseDataSource>(factory.CreateDataSource("Host=ignored"));
#endif

        // TODO
        // ClassicAssert.IsInstanceOf<ClickHouseConnectionStringBuilder>(factory.CreateCommandBuilder());
    }
}
