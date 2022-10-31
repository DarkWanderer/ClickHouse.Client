using System.Threading.Tasks;
using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests;

public static class ErrorHandlingTests
{
    [Test]
    public static async Task ExceptionHandlerShouldParseErrorCode()
    {
        using var connection = TestUtilities.GetTestClickHouseConnection(true);
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
