using System.Net;
using System.Numerics;
using ClickHouse.Client.Numerics;
using ClickHouse.Client.Tests;
using LinqToDB;
using LinqToDB.Data;
using LinqToDB.DataProvider.ClickHouse;

namespace ClickHouse.Client.IntegrationTests;

public class Tests
{
    sealed class Linq2DbTestTable
    {
        public int Id { get; set; }

        // test provider-specific type mapping
        public ClickHouseDecimal Decimal { get; set; }

        // test custom data reader Get* methods:

        // ClickHouseDataReader::GetIPAddress(int)
        public IPAddress? IPAddress { get; set; }
        // ClickHouseDataReader::GetSByte(int)
        public sbyte SByte { get; set; }
        // ClickHouseDataReader::GetUInt16(int)
        public ushort UInt16 { get; set; }
        // ClickHouseDataReader::GetUInt32(int)
        public uint UInt32 { get; set; }
        // ClickHouseDataReader::GetUInt64(int)
        public ulong UInt64 { get; set; }
        // ClickHouseDataReader::GetBigInteger(int)
        public BigInteger BigInteger { get; set; }

        public static Linq2DbTestTable[] TestData =
        [
            new Linq2DbTestTable()
        {
            Id = 1,
            Decimal = new ClickHouseDecimal(12.3M),
            IPAddress = IPAddress.Parse("1::"),
            SByte = -123,
            UInt16 = ushort.MaxValue,
            UInt32 = uint.MaxValue,
            UInt64 = ulong.MaxValue,
            BigInteger = BigInteger.Parse("567846734868672348679623672346")
        }
        ];
    }

    [Test]
    public async Task Linq2DbBulkCopy()
    {
        var connectionString = TestUtilities.GetConnectionStringBuilder().ConnectionString;

        // Covers: ClickHouseConnection::..ctor(string)
        await using var db = new DataConnection(new DataOptions().UseClickHouse(ClickHouseProvider.ClickHouseClient, connectionString));

        // cannot use temp table as we need to test WithoutSession option, incompatible with session tables
        var tb = await db.CreateTableAsync<Linq2DbTestTable>();

        try
        {
            var options = new BulkCopyOptions()
            {
                // Covers: ClickHouseBulkCopy::BatchSize
                MaxBatchSize = 10,
                // Covers: ClickHouseBulkCopy::MaxDegreeOfParallelism
                MaxDegreeOfParallelism = 1,
                // Covers:
                // ClickHouseConnectionStringBuilder::.ctor(string)
                // ClickHouseConnectionStringBuilder::UseSession
                // ClickHouseConnectionStringBuilder::ToString
                WithoutSession = true
            };

            // Covers:
            // ClickHouseBulkCopy::.ctor(ClickHouseConnection)
            // ClickHouseBulkCopy::Dispose()
            // ClickHouseBulkCopy::DestinationTableName
            // ClickHouseBulkCopy::RowsWritten
            // ClickHouseBulkCopy::InitAsync
            // ClickHouseBulkCopy::ColumnNames
            await tb.BulkCopyAsync(options, Linq2DbTestTable.TestData);

            db.InlineParameters = true;
            // Covers:
            // ClickHouseDecimal::ToString(IFormatProvider)
            var record = await tb.Where(r => r.Decimal == new ClickHouseDecimal(12.3M)).SingleAsync();

            // optional assert could be added
        }
        finally
        {
            await tb.DropAsync();
        }

        Assert.Pass();
    }
}
