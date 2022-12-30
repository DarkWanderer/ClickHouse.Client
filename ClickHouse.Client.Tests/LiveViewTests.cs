using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Client.Live;
using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    public class LiveViewTests
    {
        [Test]
        public async Task ShouldReceiveEventsWithTimer()
        {
            IList<dynamic> results = null;
            using var connection = TestUtilities.GetTestClickHouseConnection(session: true);
            await connection.ExecuteStatementAsync("CREATE LIVE VIEW IF NOT EXISTS test.live_view_timer WITH REFRESH 1 AS SELECT now();");
            using var watcher = new LiveViewWatcher(connection, "test.live_view_timer") { Limit = 1 };
            watcher.OnChangeLiveViewResult += (data) => results = data;
            await watcher.WatchAsync();

            Assert.AreEqual(watcher.Limit, watcher.Updates);
            Assert.IsNotNull(results);
            Assert.AreEqual(1, results.Count);
            CollectionAssert.IsNotEmpty(results);
        }

        [Test]
        public async Task ShouldLimitEventsFromUnlimitedStream()
        {
            IList<dynamic> results = null;
            using var connection = TestUtilities.GetTestClickHouseConnection(session: true);
            await connection.ExecuteStatementAsync("TRUNCATE TABLE IF EXISTS test.numbers_for_live_view");
            await connection.ExecuteStatementAsync("DROP VIEW IF EXISTS test.live_view_numbers");
            await connection.ExecuteStatementAsync("CREATE TABLE IF NOT EXISTS test.numbers_for_live_view (number UInt8) Engine = Memory");
            await connection.ExecuteStatementAsync("CREATE LIVE VIEW IF NOT EXISTS test.live_view_numbers AS SELECT * FROM test.numbers_for_live_view");

            using var watcher = new LiveViewWatcher(connection, "test.live_view_numbers") { Limit = 3 };
            watcher.OnChangeLiveViewResult += (data) => results = data;
            var watchTask = watcher.WatchAsync();

            using (var insertConnection = TestUtilities.GetTestClickHouseConnection())
            {
                await insertConnection.ExecuteStatementAsync($"INSERT INTO test.numbers_for_live_view VALUES (1)");
                await insertConnection.ExecuteStatementAsync($"INSERT INTO test.numbers_for_live_view VALUES (1), (2)");
                await insertConnection.ExecuteStatementAsync($"INSERT INTO test.numbers_for_live_view VALUES (1), (2), (3)");
            }
            await watchTask;

            Assert.AreEqual(watcher.Limit, watcher.Updates);
            Assert.IsNotNull(results);
            Assert.AreEqual(20, results.Count);
        }
    }
}
