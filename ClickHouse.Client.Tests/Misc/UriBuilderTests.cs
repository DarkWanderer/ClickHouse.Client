using System;
using System.Collections.Generic;
using System.Web;
using NUnit.Framework;

namespace ClickHouse.Client.Tests.Misc;

public class UriBuilderTests
{
#if !NET462 && !NET48
    [Test]
    public void ShouldSetUriParametersCorrectly()
    {
        var builder = new ClickHouseUriBuilder(new Uri("http://some.server:123"))
        {
            Database = "DATABASE",
            ConnectionQueryStringParameters = new Dictionary<string, object> { { "a", 1 }, { "b", "c" } },
            CommandQueryStringParameters = new Dictionary<string, object> { { "c", 1 }, { "d", "c" } },
            UseCompression = false,
            Sql = "SELECT 1",
            SessionId = "SESSION",
            QueryId = "QUERY",
        };

        builder.AddSqlQueryParameter("sqlParameterName", "sqlParameterValue");

        var result = new Uri(builder.ToString());
        var @params = HttpUtility.ParseQueryString(result.Query);

        Assert.AreEqual("some.server", result.Host);
        Assert.AreEqual(123, result.Port);
        Assert.AreEqual("DATABASE", @params.Get("database"));
        Assert.AreEqual("SELECT 1", @params.Get("query"));
        Assert.AreEqual("1", @params.Get("a"));
        Assert.AreEqual("c", @params.Get("b"));
        Assert.AreEqual("1", @params.Get("c"));
        Assert.AreEqual("c", @params.Get("d"));
        Assert.AreEqual("SESSION", @params.Get("session_id"));
        Assert.AreEqual("false", @params.Get("enable_http_compression"));
        Assert.AreEqual("QUERY", @params.Get("query_id"));
        Assert.AreEqual("sqlParameterValue", @params.Get("param_sqlParameterName"));
    }

    [Test]
    public void CommandQueryStringParametersShouldOverrideConnectionParameters()
    {
        var builder = new ClickHouseUriBuilder(new Uri("http://some.server:123"))
        {
            ConnectionQueryStringParameters = new Dictionary<string, object> { { "a", 1 } },
            CommandQueryStringParameters = new Dictionary<string, object> { { "a", 2 } },
        };

        var result = new Uri(builder.ToString());
        var @params = HttpUtility.ParseQueryString(result.Query);

        Assert.AreEqual("2", @params.Get("a"));
    }
#endif
}
