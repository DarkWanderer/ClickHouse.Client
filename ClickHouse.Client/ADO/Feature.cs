using System;

namespace ClickHouse.Client.ADO
{
    [Flags]
    public enum Feature
    {
        None = 0, // Special value

        InlineQuery = 2,
        DateTime64 = 4,
        Decimals = 8,
        IPv6 = 16,
        UUIDParameters = 32,
        Map = 64,
        Bool = 128,
        Date32 = 256,
        WideTypes = 512,
        Geo = 1024,

        All = ~None, // Special value
    }
}
