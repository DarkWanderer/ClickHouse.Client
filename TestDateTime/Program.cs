using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using Dapper;
using System;
using System.Collections.Generic;
using System.Linq;

namespace TestDateTime
{
    class Program
    {
        private static ClickHouseConnectionStringBuilder GetConnectionStringBuilder(string database)
        {
            var devConnectionString = $"Username=default;Password=tpk6210248;Host=10.0.0.138;Database={database};Port=8123";

            return new ClickHouseConnectionStringBuilder() { ConnectionString = devConnectionString };
        }
        public static ClickHouseConnection GetClickHouseConnection(string database = "ahe", bool compression = true)
        {
            var builder = GetConnectionStringBuilder(database);
            builder.Compression = compression;
            return new ClickHouseConnection(builder.ConnectionString);
        }

        static void Main(string[] args)
        {
            // SqlMapper.RemoveTypeMap(typeof(DateTime));
            // SqlMapper.RemoveTypeMap(typeof(DateTime?));


            var connection = GetClickHouseConnection("test");

            connection.Execute(@"drop table if exists test.test_timezone_1");

            connection.Execute(@"CREATE TABLE test.test_timezone_1
                                (
                                    `clock_01` DateTime('Asia/Tehran'),
                                    `clock_02` DateTime('Asia/Tehran'),
                                    `clock_03` DateTime('Asia/Tehran'),
                                    `clock_04` DateTime('Asia/Tehran'),
                                    `clock_05` DateTime('Asia/Tehran'),
                                    `clock_06` DateTime('Asia/Tehran'),
                                    `message` String
                                )
                                ENGINE = MergeTree
                                PARTITION BY toYYYYMM(clock_01)
                                ORDER BY (clock_01);");

            var ResponseList = new List<object[]>();

            ResponseList.Add(
                new object[]
                {
                    new DateTime(2020, 01, 01, 01, 01, 01, DateTimeKind.Local),
                    DateTime.Today,
                    DateTime.SpecifyKind(DateTime.Today, DateTimeKind.Utc),
                    DateTime.SpecifyKind(DateTime.Today, DateTimeKind.Local),
                    DateTime.Today.ToUniversalTime(),
                    DateTime.Today.ToLocalTime(),
                    "2020-01-01 01:01:01"
                    });


            ClickHouseBulkCopy bulkCopy = new ClickHouseBulkCopy(connection);
            bulkCopy.DestinationTableName = "test_timezone_1";
            var task = bulkCopy.WriteToServerAsync(ResponseList);
            task.Wait();

            var data = connection.Query(
                @"SELECT 
                clock_01,
                clock_02,
                clock_03,
                clock_04,
                clock_05,
                clock_06,
                message
                from test.test_timezone_1").ToList();

            var dd = ((IDictionary<string, object>)data[0]);

            for (int i = 0; i < dd.Values.Count; i++)
            {
                var v = dd.Values.ToList()[i];
                var str_v = (v is DateTime) ? ((DateTime)v).ToString("yyyy-MM-dd HH:mm:ss") : v.ToString();
                Console.WriteLine($"Key {dd.Keys.ToList()[i]}:\t {str_v}");
            }


            Console.ReadKey();
        }
    }
}
