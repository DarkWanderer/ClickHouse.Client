using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using ClickHouse.Client.ADO;

namespace ClickHouse.Client.Utility;

internal static class ClickHouseFeatureMap
{
    private static readonly Dictionary<Version, Feature> FeatureMap = new();

    static ClickHouseFeatureMap()
    {
        var type = typeof(Feature);
        var versionsToFeatures = from field in type.GetFields()
                                 let attribute = field.GetCustomAttribute<SinceVersionAttribute>()
                                 where attribute != null
                                 let value = (Feature)field.GetRawConstantValue()
                                 select (value, attribute.Version);

        foreach ((var feature, var version) in versionsToFeatures)
        {
            if (!FeatureMap.TryAdd(version, feature))
                FeatureMap[version] |= feature;
        }
    }

    internal static Feature GetFeatureFlags(Version serverVersion)
    {
        var result = Feature.None;
        foreach (var feature in FeatureMap)
        {
            if (serverVersion >= feature.Key)
                result |= feature.Value;
        }
        return result;
    }
}
