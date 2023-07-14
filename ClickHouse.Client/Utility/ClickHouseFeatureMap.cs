using System;
using System.Collections.Generic;
using ClickHouse.Client.ADO;

namespace ClickHouse.Client.Utility;

internal static class ClickHouseFeatureMap
{
    private static readonly Dictionary<Version, Feature> FeatureMap = new()
    {
        { new Version(20, 0), Feature.Decimals | Feature.IPv6 },
        { new Version(20, 1), Feature.DateTime64 },
        { new Version(20, 5), Feature.InlineQuery | Feature.Geo },
        { new Version(21, 4), Feature.UUIDParameters | Feature.Map },
        { new Version(21, 6), Feature.WideTypes },
        { new Version(21, 9), Feature.Date32 },
        { new Version(21, 12), Feature.Bool },
        { new Version(22, 6), Feature.Stats | Feature.Json },
        { new Version(22, 8), Feature.AsyncInsert },
    };

    internal static Feature GetFeatureFlags(Version serverVersion)
    {
        Feature result = 0;
        foreach (var feature in FeatureMap)
        {
            if (serverVersion >= feature.Key)
                result |= feature.Value;
        }
        return result;
    }
}
