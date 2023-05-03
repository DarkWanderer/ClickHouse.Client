using System;
using System.Linq;

namespace ClickHouse.Client;

/// <summary>
/// Class holding compatibility switches
/// Switch format is `ClickHouse.Client.[field name]`
/// </summary>
internal class FeatureSwitch
{
    internal const string Prefix = "ClickHouse.Client.";

    /// <summary>
    /// Disables replacement of @param to {param:DataType}
    /// ClickHouse.Client.DisableReplacingParameters
    /// </summary>
    public static bool DisableReplacingParameters;

    /// <summary>
    /// Disables usage of ClickHouseDecimal class
    /// ClickHouse.Client.DisableClickHouseDecimal
    /// </summary>
    public static bool DisableClickHouseDecimal;

    static FeatureSwitch()
    {
        Refresh();
    }

    public static void Refresh()
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
