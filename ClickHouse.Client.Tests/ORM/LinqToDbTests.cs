using ClickHouse.Client.Copy;
using NUnit.Framework;
using System;

namespace ClickHouse.Client.Tests.ORM;

public class LinqToDbTests : AbstractConnectionTestFixture
{
    [Test]
    public void ShouldBulkCopyHasConstructorWithOneParameter()
    {
        ClickHouseBulkCopy cpy = null;
        try
        {
            cpy = (ClickHouseBulkCopy)Activator.CreateInstance(typeof(ClickHouseBulkCopy), new object[] { connection });
        }
        catch
        {
            cpy = null;
        }

        Assert.IsNotNull(cpy);
    }
}
