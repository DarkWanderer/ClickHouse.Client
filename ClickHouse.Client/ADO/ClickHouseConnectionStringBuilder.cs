using System;
using System.Data.Common;
using System.Globalization;

namespace ClickHouse.Client.ADO;

public class ClickHouseConnectionStringBuilder : DbConnectionStringBuilder
{
    public ClickHouseConnectionStringBuilder()
    {
    }

    public ClickHouseConnectionStringBuilder(string connectionString)
    {
        ConnectionString = connectionString;
    }

    public string Database
    {
        get => GetStringOrDefault("Database", "default");
        set => this["Database"] = value;
    }

    public string Username
    {
        get => GetStringOrDefault("Username", "default");
        set => this["Username"] = value;
    }

    public string Password
    {
        get => GetStringOrDefault("Password", string.Empty);
        set => this["Password"] = value;
    }

    public string Protocol
    {
        get => GetStringOrDefault("Protocol", "http");
        set => this["Protocol"] = value;
    }

    public string Host
    {
        get => GetStringOrDefault("Host", "localhost");
        set => this["Host"] = value;
    }

    public bool Compression
    {
        get => GetBooleanOrDefault("Compression", true);
        set => this["Compression"] = value;
    }

    public bool UseSession
    {
        get => GetBooleanOrDefault("UseSession", false);
        set => this["UseSession"] = value;
    }

    public string SessionId
    {
        get => GetStringOrDefault("SessionId", null);
        set => this["SessionId"] = value;
    }

    public ushort Port
    {
        get => TryGetValue("Port", out var value) && value is string @string && ushort.TryParse(@string, out var @ushort) ? @ushort : (ushort)8123;
        set => this["Port"] = value;
    }

    public bool UseServerTimezone
    {
        get => GetBooleanOrDefault("UseServerTimezone", true);
        set => this["UseServerTimezone"] = value;
    }

    public bool UseCustomDecimals
    {
        get => GetBooleanOrDefault("UseCustomDecimals", false);
        set => this["UseCustomDecimals"] = value;
    }

    public TimeSpan Timeout
    {
        get
        {
            return TryGetValue("Timeout", out var value) && value is string @string && double.TryParse(@string, NumberStyles.Any, CultureInfo.InvariantCulture, out var timeout)
                ? TimeSpan.FromSeconds(timeout)
                : TimeSpan.FromMinutes(2);
        }
        set => this["Timeout"] = value.TotalSeconds;
    }

    private bool GetBooleanOrDefault(string name, bool @default)
    {
        if (TryGetValue(name, out var value))
            return "true".Equals(value as string, StringComparison.OrdinalIgnoreCase);
        else
            return @default;
    }

    private string GetStringOrDefault(string name, string @default)
    {
        if (TryGetValue(name, out var value))
            return (string)value;
        else
            return @default;
    }
}
