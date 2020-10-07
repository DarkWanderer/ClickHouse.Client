using System;
using ClickHouse.Client.ADO;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    [TestFixture]
    public class AbstractConnectionTestFixture : IDisposable
    {
        protected readonly ClickHouseConnection connection;

        protected AbstractConnectionTestFixture()
        {
            connection = TestUtilities.GetTestClickHouseConnection();
        }

        public void Dispose() => connection?.Dispose();
    }
}
