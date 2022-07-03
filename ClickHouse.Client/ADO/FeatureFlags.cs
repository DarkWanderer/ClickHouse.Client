using System;

namespace ClickHouse.Client.ADO
{
    [Flags]
    public enum FeatureFlags
    {
        SupportsInlineQuery = 2,
        SupportsDateTime64 = 4,
        SupportsDecimal = 8,
        SupportsIPv6 = 16,
        SupportsUUIDParameters = 32,
        SupportsMap = 64,
        SupportsBool = 128,
        SupportsDate32 = 256,
        SupportsWideTypes = 512,
    }
}
