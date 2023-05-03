using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ClickHouse.Client.Utility;
using Dapper;
using NUnit.Framework;

namespace ClickHouse.Client.Tests;

public class BackwardCompatibilityTests : AbstractConnectionTestFixture
{
    private readonly struct FeatureSwitchGuard : IDisposable
    {
        private readonly string @switch;

        public FeatureSwitchGuard(string name)
        {
            @switch = FeatureSwitch.Prefix + name;
            AppContext.TryGetSwitch(@switch, out bool enabled);
            Assert.IsFalse(enabled);
            AppContext.SetSwitch(@switch, true);
            FeatureSwitch.Refresh();
        }

        public void Dispose()
        {
            AppContext.SetSwitch(@switch, false);
            FeatureSwitch.Refresh();
        }
    }

    [Test]
    [NonParallelizable]
    public async Task ShouldDisableParameterSubstitution()
    {
        Assert.IsFalse(FeatureSwitch.DisableReplacingParameters);
        using var guard = new FeatureSwitchGuard(nameof(FeatureSwitch.DisableReplacingParameters));
        Assert.IsTrue(FeatureSwitch.DisableReplacingParameters);
        var sql = "SELECT @value";
        try
        {
            await connection.ExecuteScalarAsync(sql);
        }
        catch (ClickHouseServerException ex)
        {
            Assert.IsTrue(ex.Message.Contains("@value"));
        }
    }

    [Test]
    [NonParallelizable]
    public async Task ShouldDisableClickHouseDecimal()
    {
        Assert.IsFalse(FeatureSwitch.DisableClickHouseDecimal);
        using var guard = new FeatureSwitchGuard(nameof(FeatureSwitch.DisableClickHouseDecimal));
        Assert.IsTrue(FeatureSwitch.DisableClickHouseDecimal);
        // TODO: migrate from connection string parameter
    }
}
