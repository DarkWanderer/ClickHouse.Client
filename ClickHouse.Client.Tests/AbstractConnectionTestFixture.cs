using System;
using System.Text;
using ClickHouse.Client.ADO;
using NUnit.Framework;

namespace ClickHouse.Client.Tests;

[TestFixture]
public class AbstractConnectionTestFixture : IDisposable
{
    protected readonly ClickHouseConnection connection;

#if NET7_0_OR_GREATER
    protected readonly ClickHouseCancellableConnection cancellableConnection;
#endif

    protected AbstractConnectionTestFixture()
    {
        connection = TestUtilities.GetTestClickHouseConnection();
#if NET7_0_OR_GREATER
        cancellableConnection = TestUtilities.GetTestClickHouseCancellableConnection();
#endif
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
