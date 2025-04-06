using System;

namespace ClickHouse.Client;

internal class FeatureSwitch
{
    private const string Prefix = "ClickHouse.Client.";

    public static readonly bool DisableReplacingParameters;

    static FeatureSwitch()
    {
        DisableReplacingParameters = GetSwitchValue(nameof(DisableReplacingParameters));
    }

    private static bool GetSwitchValue(string switchName)
    {
        AppContext.TryGetSwitch(Prefix + switchName, out bool switchValue);
        return switchValue;
    }
}
