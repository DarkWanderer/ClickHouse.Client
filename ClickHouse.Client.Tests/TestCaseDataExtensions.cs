using ClickHouse.Client.ADO;

namespace ClickHouse.Client.Tests;

public static class TestCaseDataExtensions
{
    public static TestCaseData RequireFeature(this TestCaseData data, Feature? feature)
    {
        return !feature.HasValue || TestUtilities.SupportedFeatures.HasFlag(feature.Value)
            ? data
            : data.Ignore($"Database does not support feature {feature}");
    }
}
