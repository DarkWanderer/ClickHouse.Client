using ClickHouse.Client.Diagnostic;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClickHouse.Client.Tests.Diagnostic
{
    public class ActivitySourceHelperTest
    {
        private const string testConnectionString = "compression=True;timeout=30;database=test;username=test;password=123456;protocol=http;host=192.168.1.1;port=4567;set_session_timeout=1";
        private const string testSql = "select 1;";
        private Activity? InitActivity(bool addListener)
        {
            if (addListener)
            {
                var options = new ActivityCreationOptions<ActivityContext>();
                ActivitySource.AddActivityListener(new ActivityListener
                {
                    ShouldListenTo = source => { return true; },
                    SampleUsingParentId = (ref ActivityCreationOptions<string> activityOptions) => ActivitySamplingResult.AllData,
                    Sample = (ref ActivityCreationOptions<ActivityContext> activityOptions) => ActivitySamplingResult.AllData
                });
            }
            return ActivitySourceHelper.StartActivity("ActivitySourceHelperTest");
        }

        [TestCase(true, null, null)]
        [TestCase(true, testConnectionString, null)]
        [TestCase(true, null, testSql)]
        [TestCase(false, null, null)]
        [TestCase(false, testConnectionString, null)]
        [TestCase(false, null, testSql)]
        public void ShouldNotThrowExceptionWhenSetConnectionTags(bool addListener, string? connectionString, string? sql)
        {
            var activity = InitActivity(addListener);
            activity.SetConnectionTags(connectionString, sql);
            activity.SetSuccess();

            if (addListener)
            {
                Assert.IsNotNull(activity);

                Assert.IsNotNull(activity.GetTagItem(ActivitySourceHelper.Tag_ThreadId));
                Assert.AreEqual(ActivitySourceHelper.Value_DbSystem, activity.GetTagItem(ActivitySourceHelper.Tag_DbSystem));
                Assert.AreEqual(ActivitySourceHelper.Value_StatusOK, activity.GetTagItem(ActivitySourceHelper.Tag_StatusCode));
                if (connectionString != null)
                {
                    Assert.IsNotNull(activity.GetTagItem(ActivitySourceHelper.Tag_DbConnectionString));
                    Assert.IsNotNull(activity.GetTagItem(ActivitySourceHelper.Tag_DbName));
                    Assert.IsNotNull(activity.GetTagItem(ActivitySourceHelper.Tag_User));
                    Assert.IsNotNull(activity.GetTagItem(ActivitySourceHelper.Tag_Service));
                }
                if (sql != null)
                {
                    Assert.AreEqual(sql, activity.GetTagItem(ActivitySourceHelper.Tag_DbStatement));
                }
            }
        }

        [TestCase(true, true)]
        [TestCase(true, false)]
        [TestCase(false, true)]
        [TestCase(false, false)]
        public void ShouldNotThrowExceptionWhenSetException(bool addListener, bool exceptionIsNull)
        {
            var activity = InitActivity(addListener);
            Exception? exception = exceptionIsNull ? null : new Exception("test");
            activity.SetException(exception);

            if (addListener)
            {
                Assert.IsNotNull(activity);

                Assert.IsNotNull(activity.GetTagItem(ActivitySourceHelper.Tag_ThreadId));
                Assert.AreEqual(ActivitySourceHelper.Value_DbSystem, activity.GetTagItem(ActivitySourceHelper.Tag_DbSystem));
                Assert.AreEqual(ActivitySourceHelper.Value_StatusERROR, activity.GetTagItem(ActivitySourceHelper.Tag_StatusCode));
                if (!exceptionIsNull)
                {
                    Assert.IsNotNull(activity.GetTagItem(ActivitySourceHelper.Tag_StatusDescription));
                    Assert.True(activity.Events.Any(x => x.Name == "exception"));
                }
            }
        }
    }
}
