using System;

namespace ClickHouse.Client.ADO
{
    [Flags]
    public enum FeatureFlags
    {
        SupportsHttpParameters = 1,
        SupportsInlineQuery = 2,
        SupportsDateTime64 = 4,
        SupportsDecimal = 8,
        SupportsIPv6 = 16,
        SupportsUUIDParameters = 32,
    }
}
