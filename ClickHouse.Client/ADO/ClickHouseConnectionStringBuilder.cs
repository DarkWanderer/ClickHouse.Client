using System;
using System.Data.Common;

namespace ClickHouse.Client.ADO
{
    public class ClickHouseConnectionStringBuilder : DbConnectionStringBuilder
    {
        public ClickHouseConnectionStringBuilder()
        {
        }

        public string Database
        {
            get => base.TryGetValue("Database", out var value) ? value as string : "default";
            set => this["Database"] = value;
        }

        public string Username
        {
            get => base.TryGetValue("Username", out var value) ? value as string : "default";
            set => this["Username"] = value;
        }

        public string Password
        {
            get => base.TryGetValue("Password", out var value) ? value as string : string.Empty;
            set => this["Password"] = value;
        }

        public string Host
        {
            get => base.TryGetValue("Host", out var value) ? value as string : "localhost";
            set => this["Host"] = value;
        }

        public bool Compression
        {
            get => base.TryGetValue("Compression", out var value) ? "true".Equals(value as string, StringComparison.OrdinalIgnoreCase) : false;
            set => this["Compression"] = value;
        }

        public ushort Port
        {
            get
            {
                if (base.TryGetValue("Port", out var value) && value is string @string && ushort.TryParse(@string, out var @ushort))
                    return @ushort;
                return 8123;
            }
            set => this["Port"] = value;
        }

        public ClickHouseConnectionDriver Driver
        {
            get
            {
                if (base.TryGetValue("Driver", out var value) && value is string @string && Enum.TryParse<ClickHouseConnectionDriver>(@string, out var @enum))
                    return @enum;
                return ClickHouseConnectionDriver.Binary;
            }
            set => this["Driver"] = value.ToString();
        }
    }
}