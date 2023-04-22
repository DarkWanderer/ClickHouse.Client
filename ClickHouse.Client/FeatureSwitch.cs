using System;
using System.Linq;

namespace ClickHouse.Client;

internal class FeatureSwitch
{
    private const string Prefix = "ClickHouse.Client.";

    // Field names are used as switch
    public static readonly bool DisableReplacingParameters;

    static FeatureSwitch()
    {
        var fields = typeof(FeatureSwitch).GetFields().Where(f => f.FieldType == typeof(bool));
        foreach (var field in fields)
        {
            var switchName = Prefix + field.Name;
            AppContext.TryGetSwitch(switchName, out bool switchValue);
            field.SetValue(null, switchValue);
        }
    }
}
