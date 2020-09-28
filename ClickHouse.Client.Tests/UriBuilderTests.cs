using System;
using System.Collections.Generic;
using System.Web;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    public class UriBuilderTests
    {
        [Test]
        public void ShouldSetUriParametersCorrectly()
        {
            var builder = new ClickHouseUriBuilder(new Uri("http://some.server:123"))
            {
                Database = "DATABASE",
                CustomParameters = new Dictionary<string, object> { { "a", 1 }, { "b", "c" } },
                UseCompression = false,
                Sql = "SELECT 1",
                SessionId = "SESSION"
            };

            var result = new Uri( builder.ToString());
            var @params = HttpUtility.ParseQueryString(result.Query);

            Assert.AreEqual("some.server", result.Host);
            Assert.AreEqual(123, result.Port);
            Assert.AreEqual("DATABASE", @params.Get("database"));
            Assert.AreEqual("SELECT 1", @params.Get("query"));
            Assert.AreEqual("1", @params.Get("a"));
            Assert.AreEqual("c", @params.Get("b"));
            Assert.AreEqual("SESSION", @params.Get("session_id"));
            Assert.AreEqual("false", @params.Get("enable_http_compression"));
        }
    }
}
