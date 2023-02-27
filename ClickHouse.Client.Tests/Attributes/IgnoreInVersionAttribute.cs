using System;
using NUnit.Framework;
using NUnit.Framework.Interfaces;
using NUnit.Framework.Internal;

namespace ClickHouse.Client.Tests.Attributes;

/// <summary>
/// Ignores test if launched on specific ClickHouse version
/// </summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = true, Inherited = false)]
public class IgnoreInVersionAttribute : NUnitAttribute, IApplyToTest
{
    private readonly Version version;

    public IgnoreInVersionAttribute(string version)
    {
        this.version = Version.Parse(version);
    }

    public void ApplyToTest(Test test)
    {
        if (test.RunState == RunState.NotRunnable)
            return;

        var server = TestUtilities.ServerVersion;

        if (version.Major != server.Major)
            return;
        if (version.Minor != -1 && version.Minor != server.Minor)
            return;
        if (version.Build != -1 && version.Minor != server.Minor)
            return;
        if (version.Revision != -1 && version.Revision != server.Revision)
            return;

        if (TestUtilities.ServerVersion == version)
        {
            test.RunState = RunState.Ignored;
            test.MakeTestResult().RecordAssertion(new AssertionResult(AssertionStatus.Inconclusive, $"Test ignored in ClickHouse version {version}", null));
        }
    }
}
