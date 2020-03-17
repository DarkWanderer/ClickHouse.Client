using System;
using System.Data.Common;
using System.Globalization;

namespace ClickHouse.Client.ADO
{
    public class ClickHouseConnectionStringBuilder : DbConnectionStringBuilder
    {
        public ClickHouseConnectionStringBuilder()
        {
        }

        public string Database
        {
            get => TryGetValue("Database", out var value) ? value as string : "default";
            set => this["Database"] = value;
        }

        public string Username
        {
            get => TryGetValue("Username", out var value) ? value as string : "default";
            set => this["Username"] = value;
        }

        public string Password
        {
            get => TryGetValue("Password", out var value) ? value as string : string.Empty;
            set => this["Password"] = value;
        }

        public string Host
        {
            get => TryGetValue("Host", out var value) ? value as string : "localhost";
            set => this["Host"] = value;
        }

        public bool Compression
        {
            get => TryGetValue("Compression", out var value) ? "true".Equals(value as string, StringComparison.OrdinalIgnoreCase) : false;
            set => this["Compression"] = value;
        }

        public bool UseSession
        {
            get
            {
                bool useSession = TryGetValue("UseSession", out var value) ? "true".Equals(value as string, StringComparison.OrdinalIgnoreCase) : false;
                if (useSession && SessionId == null)
                    SessionId = Guid.NewGuid().ToString();

                return useSession;

            }
            set
            {
                this["UseSession"] = value;

                if (value && SessionId == null)
                    SessionId = Guid.NewGuid().ToString();


            }
        }

        public string SessionId
        {
            get
            {
                object sfff = "";
                bool isSet = TryGetValue("SessionId", out sfff);
                if (!isSet)
                    return null;

                return this["SessionId"].ToString();
            }
            set
            {
                this["SessionId"] = value;
                UseSession = value == null ? false : true;
            }
        }

        public ushort Port
        {
            get
            {
                if (TryGetValue("Port", out var value) && value is string @string && ushort.TryParse(@string, out var @ushort))
                {
                    return @ushort;
                }

                return 8123;
            }
            set => this["Port"] = value;
        }

        public ClickHouseConnectionDriver Driver
        {
            get
            {
                if (TryGetValue("Driver", out var value) && value is string @string && Enum.TryParse<ClickHouseConnectionDriver>(@string, out var @enum))
                {
                    return @enum;
                }

                return ClickHouseConnectionDriver.Binary;
            }
            set => this["Driver"] = value.ToString();
        }

        public TimeSpan Timeout
        {
            get
            {
                if (TryGetValue("Timeout", out var value) && value is string @string && double.TryParse(@string, NumberStyles.Any, CultureInfo.InvariantCulture, out var timeout))
                {
                    return TimeSpan.FromSeconds(timeout);
                }

                return TimeSpan.FromMinutes(2);
            }
            set => this["Timeout"] = value.TotalSeconds;
        }
    }
}
