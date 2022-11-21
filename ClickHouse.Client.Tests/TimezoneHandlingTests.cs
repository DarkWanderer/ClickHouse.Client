﻿using System;
using System.Globalization;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests;

[TestFixture]
public class ServerTimezoneDateTimeTests : IDisposable
{
    protected readonly ClickHouseConnection connection;

    public ServerTimezoneDateTimeTests()
    {
        var builder = TestUtilities.GetConnectionStringBuilder();
        builder.UseServerTimezone = true;
        connection = new ClickHouseConnection(builder.ToString());
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "CREATE DATABASE IF NOT EXISTS test;";
        command.ExecuteScalar();
    }

    public void Dispose() => connection?.Dispose();

    [Test]
    public async Task ShouldCorrectlyDetermineServerTimezone()
    {
        var timezone = (string)await connection.ExecuteScalarAsync("SELECT timezone()");
        Assert.AreEqual(timezone, connection.ServerTimezone);
    }

    [Test]
    public async Task ShouldRoundtripUnspecifiedDateTime()
    {
        var dt = new DateTime(2022, 06, 13, 02, 00, 00, DateTimeKind.Unspecified);
        var query = $"SELECT parseDateTimeBestEffort('{dt.ToString("s", CultureInfo.InvariantCulture)}')";
        Assert.AreEqual(dt, await connection.ExecuteScalarAsync(query));
    }

    [Test]
    public async Task ShouldReturnUTCDateTime()
    {
        var query = $"select toDateTime('2020/11/10 00:00:00', 'Etc/UTC')";
        Assert.AreEqual(new DateTime(2020, 11, 10, 00, 00, 00, DateTimeKind.Utc), await connection.ExecuteScalarAsync(query));
    }
}
