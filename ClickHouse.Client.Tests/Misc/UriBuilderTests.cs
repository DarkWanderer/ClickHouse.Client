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

    [Test]
    public void ConnectionQueryStringParametersShouldOverrideCommonParameters()
    {
        var builder = new ClickHouseUriBuilder(new Uri("http://some.server:123"))
        {
            Database = "DATABASE",
            UseCompression = false,
            Sql = "SELECT 1",
            SessionId = "SESSION",
            QueryId = "QUERY",
            ConnectionQueryStringParameters = new Dictionary<string, object>
            {
                { "database", "overrided" },
                { "enable_http_compression", "overrided" },
                { "query", "overrided" },
                { "session_id", "overrided" },
                { "query_id", "overrided" },
            },
        };

        builder.AddSqlQueryParameter("sqlParameterName", "sqlParameterValue");

        var result = new Uri(builder.ToString());
        var @params = HttpUtility.ParseQueryString(result.Query);

        Assert.AreEqual("overrided", @params.Get("database"));
        Assert.AreEqual("overrided", @params.Get("enable_http_compression"));
        Assert.AreEqual("overrided", @params.Get("query"));
        Assert.AreEqual("overrided", @params.Get("session_id"));
        Assert.AreEqual("overrided", @params.Get("query_id"));
    }

    [Test]
    public void ConnectionQueryStringParametersShouldOverrideSqlQueryParameters()
    {
        var builder = new ClickHouseUriBuilder(new Uri("http://some.server:123"))
        {
            ConnectionQueryStringParameters = new Dictionary<string, object>
            {
                { "param_sqlParameterName", "overrided" },
            },
        };

        builder.AddSqlQueryParameter("sqlParameterName", "sqlParameterValue");

        var result = new Uri(builder.ToString());
        var @params = HttpUtility.ParseQueryString(result.Query);

        Assert.AreEqual("overrided", @params.Get("param_sqlParameterName"));
    }

    [Test]
    public void CommandQueryStringParametersShouldOverrideCommonParameters()
    {
        var builder = new ClickHouseUriBuilder(new Uri("http://some.server:123"))
        {
            Database = "DATABASE",
            UseCompression = false,
            Sql = "SELECT 1",
            SessionId = "SESSION",
            QueryId = "QUERY",
            CommandQueryStringParameters = new Dictionary<string, object>
            {
                { "database", "overrided" },
                { "enable_http_compression", "overrided" },
                { "query", "overrided" },
                { "session_id", "overrided" },
                { "query_id", "overrided" },
            },
        };

        builder.AddSqlQueryParameter("sqlParameterName", "sqlParameterValue");

        var result = new Uri(builder.ToString());
        var @params = HttpUtility.ParseQueryString(result.Query);

        Assert.AreEqual("overrided", @params.Get("database"));
        Assert.AreEqual("overrided", @params.Get("enable_http_compression"));
        Assert.AreEqual("overrided", @params.Get("query"));
        Assert.AreEqual("overrided", @params.Get("session_id"));
        Assert.AreEqual("overrided", @params.Get("query_id"));
    }

    [Test]
    public void CommandQueryStringParametersShouldOverrideSqlQueryParameters()
    {
        var builder = new ClickHouseUriBuilder(new Uri("http://some.server:123"))
        {
            CommandQueryStringParameters = new Dictionary<string, object>
            {
                { "param_sqlParameterName", "overrided" },
            },
        };

        builder.AddSqlQueryParameter("sqlParameterName", "sqlParameterValue");

        var result = new Uri(builder.ToString());
        var @params = HttpUtility.ParseQueryString(result.Query);

        Assert.AreEqual("overrided", @params.Get("param_sqlParameterName"));
    }

    [Test]
    [TestCase("Çay", "%c3%87ay")]
    public void ShouldEncodeUnicodeCharactersCorrectly(string input, string expected)
    {
        var builder = new ClickHouseUriBuilder(new Uri("http://a.b:123"))
        {
            CommandQueryStringParameters = new Dictionary<string, object>
            {
                { "param_input", input },
            },
        };

        Assert.That(builder.ToString(), Contains.Substring(expected));
    }
#endif
}
