using System;
using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using ClickHouse.Client.ADO;

namespace ClickHouse.Client.Diagnostic;

internal static class ActivitySourceHelper
{
    internal const string ActivitySourceName = "ClickHouse.Client";

    private const string TagDbConnectionString = "db.connection_string";
    private const string TagDbName = "db.name";
    private const string TagDbStatement = "db.statement";
    private const string TagDbSystem = "db.system";
    private const string TagStatusCode = "otel.status_code";
    private const string TagUser = "db.user";
    private const string TagService = "peer.service";
    private const string TagThreadId = "thread.id";
    private const string TagReadRows = "db.clickhouse.read_rows";
    private const string TagReadBytes = "db.clickhouse.read_bytes";
    private const string TagWrittenRows = "db.clickhouse.written_rows";
    private const string TagWrittenBytes = "db.clickhouse.written_bytes";
    private const string TagResultRows = "db.clickhouse.result_rows";
    private const string TagResultBytes = "db.clickhouse.result_bytes";
    private const string TagElapsedNs = "db.clickhouse.elapsed_ns";

    internal const int StatementMaxLen = 300;

    internal static ActivitySource ActivitySource { get; } = CreateActivitySource();

    internal static Activity StartActivity(this ClickHouseConnection connection, string name)
    {
        if (connection is null) throw new ArgumentNullException(nameof(connection));
        if (string.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));

        var activity = ActivitySource.StartActivity(name, ActivityKind.Client, default(ActivityContext));

        if (activity is null)
            return null;

        if (activity.IsAllDataRequested)
        {
            activity.SetTag(TagThreadId, Environment.CurrentManagedThreadId.ToString(CultureInfo.InvariantCulture));
            activity.SetTag(TagDbSystem, "clickhouse");
        }
        activity.SetTag(TagDbConnectionString, connection.RedactedConnectionString);
        activity.SetTag(TagDbName, connection.Database);
        activity.SetTag(TagUser, connection.Username);
        activity.SetTag(TagService, $"{connection.ServerUri.Host}:{connection.ServerUri.Port}{connection.ServerUri.AbsolutePath}");
        return activity;
    }

    internal static void SetQuery(this Activity activity, string sql)
    {
        if (activity is null || sql is null)
            return;
        if (sql.Length > StatementMaxLen)
        {
            sql = sql.Substring(0, StatementMaxLen);
        }
        activity.SetTag(TagDbStatement, sql);
    }

    internal static void SetQueryStats(this Activity activity, QueryStats stats)
    {
        if (activity is null || stats is null)
            return;
        activity.SetTag(TagReadRows, stats.ReadRows);
        activity.SetTag(TagReadBytes, stats.ReadBytes);
        activity.SetTag(TagWrittenRows, stats.WrittenRows);
        activity.SetTag(TagWrittenBytes, stats.WrittenBytes);
        activity.SetTag(TagResultRows, stats.ResultRows);
        activity.SetTag(TagResultBytes, stats.ResultBytes);
        activity.SetTag(TagElapsedNs, stats.ElapsedNs);
    }

    internal static void SetSuccess(this Activity activity)
    {
#if NET6_0_OR_GREATER
        activity?.SetStatus(ActivityStatusCode.Ok);
#endif
        activity?.SetTag(TagStatusCode, "OK");
        activity?.Stop();
    }

    internal static void SetException(this Activity activity, Exception exception)
    {
        if (exception is null) throw new ArgumentNullException(nameof(exception));

        var description = exception.Message;
#if NET6_0_OR_GREATER
        activity?.SetStatus(ActivityStatusCode.Error, description);
#endif
        activity?.SetTag(TagStatusCode, "ERROR");
        activity?.SetTag("otel.status_description", description);
        activity?.AddEvent(new ActivityEvent("exception", tags: new ActivityTagsCollection
        {
            { "exception.type", exception?.GetType().FullName },
            { "exception.message", exception?.Message },
        }));
        activity?.Stop();
    }

    private static ActivitySource CreateActivitySource()
    {
        var assembly = typeof(ActivitySourceHelper).Assembly;
        var version = assembly.GetCustomAttribute<AssemblyFileVersionAttribute>().Version;
        return new ActivitySource(ActivitySourceName, version);
    }
}
