using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests.Misc
{
    public class ExtensionsTests
    {
        [Test]
        public void ShouldDeconstruct()
        {
            var list = new[] { 1, 2 };
            var (first, _) = list;
            Assert.AreEqual(1, first);
        }
    }
}
