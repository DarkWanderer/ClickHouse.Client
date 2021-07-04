using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    public class DateTimeTests
    {
        private static readonly DateTime testDateTime = new DateTime(2020, 1, 1, 0, 0, 0, 0, DateTimeKind.Unspecified);
        private static readonly TimeSpan testOffset = new TimeSpan(3, 0, 0);
        private static readonly DateTimeOffset testDto = new DateTimeOffset(testDateTime, testOffset);

        [Test]
        public async Task SelectMoscowDateTime()
        {
            var builder = TestUtilities.GetConnectionStringBuilder();
            using var connection = new ClickHouseConnection(builder.ToString());
            await connection.OpenAsync();
            var result = await connection.ExecuteScalarAsync("select toDateTime('2020-01-01 00:00:00', 'Europe/Moscow')");
            Assert.AreEqual(testDto, result);
        }

        [Test]
        public async Task SelectUnspecifiedDateTime()
        {
            var builder = TestUtilities.GetConnectionStringBuilder();
            using var connection = new ClickHouseConnection(builder.ToString());
            await connection.OpenAsync();
            var result = (DateTimeOffset)await connection.ExecuteScalarAsync("select toDateTime('2020-01-01 00:00:00')");

            // Not checking offset as it will be server timezone dependent
            Assert.AreEqual(testDateTime, result.DateTime);
        }

        [Test]
        public async Task SelectMoscowDateTimeAsParameter()
        {
            var builder = TestUtilities.GetConnectionStringBuilder();
            using var connection = new ClickHouseConnection(builder.ToString());
            await connection.OpenAsync();
            using var command = connection.CreateCommand();
            command.AddParameter("input", testDto);
            command.CommandText = "select {input:DateTime('Europe/Moscow')}";
            var result = await command.ExecuteScalarAsync();
            Assert.AreEqual(testDto, result);
        }

        [Test]
        public async Task SelectUnspecifiedDateTimeAsParameter()
        {
            var builder = TestUtilities.GetConnectionStringBuilder();
            using var connection = new ClickHouseConnection(builder.ToString());
            await connection.OpenAsync();
            using var command = connection.CreateCommand();
            command.AddParameter("input", testDto);
            command.CommandText = "select {input:DateTime}";
            var result = (DateTimeOffset)await command.ExecuteScalarAsync();

            // Not checking offset as it will be server timezone dependent
            Assert.AreEqual(testDateTime, result.DateTime); 
        }
    }
}
