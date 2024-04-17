using System;
using NUnit.Framework;
using NUnit.Framework.Interfaces;
using NUnit.Framework.Internal;

namespace ClickHouse.Client.Tests.Attributes;

/// <summary>
/// Ignores test if launched on specific ClickHouse version
/// </summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = false)]
public class FromVersionAttribute : NUnitAttribute, IApplyToTest
{
    private Version version;

    public FromVersionAttribute(int major, int minor = 0, int build = 0, int revision = 0)
    {
        version = new Version(major, minor, build, revision);
    }

    public void ApplyToTest(Test test)
    {
        if (test.RunState == RunState.NotRunnable)
            return;

        var server = TestUtilities.ServerVersion;

        if (server == null)
            return; // 'latest' version

        if (version <= server)
            return;

        test.RunState = RunState.Ignored;
        test.MakeTestResult().RecordAssertion(new AssertionResult(AssertionStatus.Inconclusive, $"Test ignored, server version {server} is less than {version}", null));
    }
}
