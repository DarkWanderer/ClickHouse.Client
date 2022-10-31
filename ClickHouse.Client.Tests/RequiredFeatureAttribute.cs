using System;
using ClickHouse.Client.ADO;
using NUnit.Framework;
using NUnit.Framework.Interfaces;
using NUnit.Framework.Internal;

namespace ClickHouse.Client.Tests;

[AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = false)]
public class RequiredFeatureAttribute : NUnitAttribute, IApplyToTest
{
    private readonly Feature feature;

    public RequiredFeatureAttribute(Feature feature)
    {
        this.feature = feature;
    }

    public void ApplyToTest(Test test)
    {
        if (test.RunState == RunState.NotRunnable)
        {
            return;
        }

        if (!TestUtilities.SupportedFeatures.HasFlag(feature))
        {
            test.RunState = RunState.Ignored;
            test.MakeTestResult().RecordAssertion(new AssertionResult(AssertionStatus.Inconclusive, $"Test requires feature {feature} but database does not support it", null));
        }
    }
}
