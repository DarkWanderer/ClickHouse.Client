using ClickHouse.Client.ADO;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Reflection;

namespace ClickHouse.Client.Diagnostic
{
    internal static class ActivitySourceHelper
    {
        internal const string ActivitySourceName = "ClickHouse.Client";

        internal const string Tag_DbConnectionString = "db.connection_string";
        internal const string Tag_DbName = "db.name";
        internal const string Tag_DbStatement = "db.statement";
        internal const string Tag_DbSystem = "db.system";
        internal const string Tag_StatusCode = "otel.status_code";
        internal const string Tag_User = "db.user";
        internal const string Tag_Service = "peer.service";
        internal const string Tag_ThreadId = "thread.id";

        internal const string Value_DbSystem = "clickhouse";
        internal const int StatementMaxLen = 300;

        internal static Activity? StartActivity(string name)
        {
            var activity = ActivitySource.StartActivity(name, ActivityKind.Client, default(ActivityContext));
            if (activity is { IsAllDataRequested: true })
            {
                activity.SetTag(Tag_ThreadId, Environment.CurrentManagedThreadId.ToString(CultureInfo.InvariantCulture));
                activity.SetTag(Tag_DbSystem, Value_DbSystem);
            }
            return activity;
        }

        internal static void SetConnectionTags(this Activity? activity, string connectionString, string? sql)
        {
            var builder = new ClickHouseConnectionStringBuilder() { ConnectionString = connectionString };
            activity?.SetTag(Tag_DbConnectionString, connectionString);
            activity?.SetTag(Tag_DbName, builder.Database);
            activity?.SetTag(Tag_User, builder.Username);
            activity?.SetTag(Tag_Service, new UriBuilder(builder.Protocol, builder.Host, builder.Port).Uri.ToString());
            activity?.SetTag(Tag_DbStatement, sql is not null && sql.Length > StatementMaxLen ? sql.Substring(0, StatementMaxLen) : sql);
        }

        internal static void SetSuccess(this Activity? activity)
        {
#if NET6_0_OR_GREATER
            activity?.SetStatus(ActivityStatusCode.Ok);
#endif
            activity?.SetTag(Tag_StatusCode, "OK");
            activity?.Stop();
        }

        internal static void SetException(this Activity? activity, Exception? exception)
        {
            var description = exception.Message;
#if NET6_0_OR_GREATER
            activity?.SetStatus(ActivityStatusCode.Error, description);
#endif
            activity?.SetTag(Tag_StatusCode, "ERROR");
            activity?.SetTag("otel.status_description", description);
            activity?.AddEvent(new ActivityEvent("exception", tags: new ActivityTagsCollection
            {
                { "exception.type", exception?.GetType().FullName },
                { "exception.message", exception?.Message },
            }));
            activity?.Stop();
        }

        private static ActivitySource ActivitySource { get; } = CreateActivitySource();

        private static ActivitySource CreateActivitySource()
        {
            var assembly = typeof(ActivitySourceHelper).Assembly;
            var version = assembly.GetCustomAttribute<AssemblyFileVersionAttribute>()!.Version;
            return new ActivitySource(ActivitySourceName, version);
        }
    }
}
