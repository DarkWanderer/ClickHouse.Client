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
    int major;
    int minor;
    int build;
    int revision;

    public IgnoreInVersionAttribute(int major, int minor = -1, int build = -1, int revision = -1)
    {
        this.major = major;
        this.minor = minor;
        this.build = build;
        this.revision = revision;
    }

    public void ApplyToTest(Test test)
    {
        if (test.RunState == RunState.NotRunnable)
            return;

        var server = TestUtilities.ServerVersion;

        if (server == null)
            return; // 'latest' version

        if (major != server.Major)
            return;
        if (minor != -1 && minor != server.Minor)
            return;
        if (build != -1 && minor != server.Minor)
            return;
        if (revision != -1 && revision != server.Revision)
            return;

        test.RunState = RunState.Ignored;
        test.MakeTestResult().RecordAssertion(new AssertionResult(AssertionStatus.Inconclusive, $"Test ignored in ClickHouse version {server}", null));
    }
}
