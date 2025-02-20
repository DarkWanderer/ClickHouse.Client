using System;
using System.Text;
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

    protected static string SanitizeTableName(string input)
    {
        var builder = new StringBuilder();
        foreach (var c in input)
        {
            if (char.IsLetterOrDigit(c) || c == '_')
                builder.Append(c);
        }

        return builder.ToString();
    }

    [OneTimeTearDown]
    public void Dispose() => connection?.Dispose();
}
