using System;
using System.Collections.Generic;
using System.Web;

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

        ClassicAssert.AreEqual("some.server", result.Host);
        ClassicAssert.AreEqual(123, result.Port);
        ClassicAssert.AreEqual("DATABASE", @params.Get("database"));
        ClassicAssert.AreEqual("SELECT 1", @params.Get("query"));
        ClassicAssert.AreEqual("1", @params.Get("a"));
        ClassicAssert.AreEqual("c", @params.Get("b"));
        ClassicAssert.AreEqual("1", @params.Get("c"));
        ClassicAssert.AreEqual("c", @params.Get("d"));
        ClassicAssert.AreEqual("SESSION", @params.Get("session_id"));
        ClassicAssert.AreEqual("false", @params.Get("enable_http_compression"));
        ClassicAssert.AreEqual("QUERY", @params.Get("query_id"));
        ClassicAssert.AreEqual("sqlParameterValue", @params.Get("param_sqlParameterName"));
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

        ClassicAssert.AreEqual("2", @params.Get("a"));
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

        ClassicAssert.AreEqual("overrided", @params.Get("database"));
        ClassicAssert.AreEqual("overrided", @params.Get("enable_http_compression"));
        ClassicAssert.AreEqual("overrided", @params.Get("query"));
        ClassicAssert.AreEqual("overrided", @params.Get("session_id"));
        ClassicAssert.AreEqual("overrided", @params.Get("query_id"));
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

        ClassicAssert.AreEqual("overrided", @params.Get("param_sqlParameterName"));
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

        ClassicAssert.AreEqual("overrided", @params.Get("database"));
        ClassicAssert.AreEqual("overrided", @params.Get("enable_http_compression"));
        ClassicAssert.AreEqual("overrided", @params.Get("query"));
        ClassicAssert.AreEqual("overrided", @params.Get("session_id"));
        ClassicAssert.AreEqual("overrided", @params.Get("query_id"));
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

        ClassicAssert.AreEqual("overrided", @params.Get("param_sqlParameterName"));
    }
#endif
}
