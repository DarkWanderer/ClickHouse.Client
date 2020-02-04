using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    public static class ErrorHandlingTests
    {
        [Test]
        public static async Task ExceptionHandlerShouldParseErrorCode()
        {
            using var connection = TestUtilities.GetTestClickHouseConnection(ADO.ClickHouseConnectionDriver.Binary, true);
            try
            {
                var result = await connection.ExecuteScalarAsync("SELECT A");
            }
            catch (ClickHouseServerException ex)
            {
                Assert.AreEqual(47, ex.ErrorCode);
            }
        }
    }
}
