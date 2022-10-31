using System;
using ClickHouse.Client.ADO;
using NUnit.Framework;

namespace ClickHouse.Client.Tests;

[TestFixture]
public class AbstractConnectionTestFixture : IDisposable
{
    protected readonly ClickHouseConnection connection;

    protected AbstractConnectionTestFixture()
    {
        connection = TestUtilities.GetTestClickHouseConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "CREATE DATABASE IF NOT EXISTS test;";
        command.ExecuteScalar();
    }

    public void Dispose() => connection?.Dispose();
}
