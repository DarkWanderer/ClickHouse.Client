using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    public class DefaultSettingsTests
    {
        [Test]
        public void DefaultSettingsShouldMatch()
        {
            var builder = new ClickHouseConnectionStringBuilder();
            Assert.AreEqual(true, builder.UseCustomDecimals);
            Assert.AreEqual(true, builder.Compression);
            Assert.AreEqual(true, builder.UseServerTimezone);
            Assert.AreEqual(false, builder.UseSession);
        }
    }
}
