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

        Assert.Multiple(() =>
        {
            Assert.That(result.Host, Is.EqualTo("some.server"));
            Assert.That(result.Port, Is.EqualTo(123));
            Assert.That(@params.Get("database"), Is.EqualTo("DATABASE"));
            Assert.That(@params.Get("query"), Is.EqualTo("SELECT 1"));
            Assert.That(@params.Get("a"), Is.EqualTo("1"));
            Assert.That(@params.Get("b"), Is.EqualTo("c"));
            Assert.That(@params.Get("c"), Is.EqualTo("1"));
            Assert.That(@params.Get("d"), Is.EqualTo("c"));
            Assert.That(@params.Get("session_id"), Is.EqualTo("SESSION"));
            Assert.That(@params.Get("enable_http_compression"), Is.EqualTo("false"));
            Assert.That(@params.Get("query_id"), Is.EqualTo("QUERY"));
            Assert.That(@params.Get("param_sqlParameterName"), Is.EqualTo("sqlParameterValue"));
        });
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

        Assert.That(@params.Get("a"), Is.EqualTo("2"));
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

        Assert.Multiple(() =>
        {
            Assert.That(@params.Get("database"), Is.EqualTo("overrided"));
            Assert.That(@params.Get("enable_http_compression"), Is.EqualTo("overrided"));
            Assert.That(@params.Get("query"), Is.EqualTo("overrided"));
            Assert.That(@params.Get("session_id"), Is.EqualTo("overrided"));
            Assert.That(@params.Get("query_id"), Is.EqualTo("overrided"));
        });
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

        Assert.That(@params.Get("param_sqlParameterName"), Is.EqualTo("overrided"));
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

        Assert.Multiple(() =>
        {
            Assert.That(@params.Get("database"), Is.EqualTo("overrided"));
            Assert.That(@params.Get("enable_http_compression"), Is.EqualTo("overrided"));
            Assert.That(@params.Get("query"), Is.EqualTo("overrided"));
            Assert.That(@params.Get("session_id"), Is.EqualTo("overrided"));
            Assert.That(@params.Get("query_id"), Is.EqualTo("overrided"));
        });
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

        Assert.That(@params.Get("param_sqlParameterName"), Is.EqualTo("overrided"));
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
