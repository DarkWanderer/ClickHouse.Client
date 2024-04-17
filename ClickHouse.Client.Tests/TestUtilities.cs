using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Net;
using System.Numerics;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Numerics;
using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests;

public static class TestUtilities
{
    public static readonly Feature SupportedFeatures;
    public static readonly Version ServerVersion;

    static TestUtilities()
    {
        var versionString = Environment.GetEnvironmentVariable("CLICKHOUSE_VERSION");
        if (versionString is not null and not "latest")
        {
            ServerVersion = Version.Parse(versionString.Split(':').Last().Trim());
            SupportedFeatures = ClickHouseFeatureMap.GetFeatureFlags(ServerVersion);
        }
        else
        {
            SupportedFeatures = Feature.All;
            ServerVersion = null;
        }
    }

    [Test]
    public static async Task ExpectedFeaturesShouldMatchActualFeatures()
    {
        using var connection = GetTestClickHouseConnection();
        await connection.OpenAsync();
        Assert.AreEqual(SupportedFeatures & connection.SupportedFeatures, connection.SupportedFeatures);
    }

    /// <summary>
    /// Utility method to allow to redirect ClickHouse connections to different machine, in case of Windows development environment
    /// </summary>
    /// <returns></returns>
    public static ClickHouseConnection GetTestClickHouseConnection(bool compression = true, bool session = false, bool customDecimals = true)
    {
        var builder = GetConnectionStringBuilder();
        builder.Compression = compression;
        builder.UseSession = session;
        builder.UseCustomDecimals = customDecimals;
        builder["set_session_timeout"] = 1; // Expire sessions quickly after test
        builder["set_allow_experimental_geo_types"] = 1; // Allow support for geo types
        builder["set_flatten_nested"] = 0; // Nested should be a single column, see https://clickhouse.com/docs/en/operations/settings/settings#flatten-nested

        if (SupportedFeatures.HasFlag(Feature.Map))
        {
            builder["set_allow_experimental_map_type"] = 1;
        }
        if (SupportedFeatures.HasFlag(Feature.Variant))
        {
            builder["set_allow_experimental_variant_type"] = 1;
        }
        return new ClickHouseConnection(builder.ConnectionString);
    }

    public static ClickHouseConnectionStringBuilder GetConnectionStringBuilder()
    {
        // Connection string must be provided pointing to a test ClickHouse server
        var devConnectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION") ??
            throw new InvalidOperationException("Must set CLICKHOUSE_CONNECTION environment variable pointing at ClickHouse server");

        return new ClickHouseConnectionStringBuilder(devConnectionString);
    }

    public readonly struct DataTypeSample(string clickHouseType, Type frameworkType, string exampleExpression, object exampleValue)
    {
        public readonly string ClickHouseType = clickHouseType;
        public readonly Type FrameworkType = frameworkType;
        public readonly string ExampleExpression = exampleExpression;
        public readonly object ExampleValue = exampleValue;
    }

    public static IEnumerable<DataTypeSample> GetDataTypeSamples()
    {
        yield return new DataTypeSample("Nothing", typeof(DBNull), "NULL", DBNull.Value);

        yield return new DataTypeSample("Int8", typeof(sbyte), "toInt8(-8)", -8);
        yield return new DataTypeSample("UInt8", typeof(byte), "toUInt8(8)", 8);

        yield return new DataTypeSample("Int16", typeof(short), "toInt16(-16)", -16);
        yield return new DataTypeSample("UInt16", typeof(ushort), "toUInt16(16)", 16);

        yield return new DataTypeSample("Int32", typeof(int), "toInt16(-32)", -32);
        yield return new DataTypeSample("UInt32", typeof(uint), "toUInt16(32)", 32);

        yield return new DataTypeSample("Int64", typeof(long), "toInt64(-64)", -64);
        yield return new DataTypeSample("UInt64", typeof(ulong), "toUInt64(64)", 64);

        yield return new DataTypeSample("Float32", typeof(float), "toFloat32(32e6)", 32e6);
        yield return new DataTypeSample("Float32", typeof(float), "toFloat32(-32e6)", -32e6);

        yield return new DataTypeSample("Float64", typeof(double), "toFloat64(64e6)", 64e6);
        yield return new DataTypeSample("Float64", typeof(double), "toFloat64(-64e6)", -64e6);

        yield return new DataTypeSample("String", typeof(string), "'TestString'", "TestString");
        yield return new DataTypeSample("String", typeof(string), "'\t\r\n'", "\t\r\n");
        // yield return new DataTypeSample("String", typeof(string), "'1\t2\n3'", "1\t2\n3");
        yield return new DataTypeSample("FixedString(3)", typeof(string), "toFixedString('ASD',3)", "ASD");
        yield return new DataTypeSample("FixedString(5)", typeof(string), "toFixedString('ASD',5)", "ASD\0\0");

        yield return new DataTypeSample("UUID", typeof(Guid), "toUUID('00000000-0000-0000-0000-000000000000')", new Guid("00000000-0000-0000-0000-000000000000"));
        yield return new DataTypeSample("UUID", typeof(Guid), "toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')", new Guid("61f0c404-5cb3-11e7-907b-a6006ad3dba0"));

        yield return new DataTypeSample("IPv4", typeof(IPAddress), "toIPv4('1.2.3.4')", IPAddress.Parse("1.2.3.4"));
        yield return new DataTypeSample("IPv4", typeof(IPAddress), "toIPv4('255.255.255.255')", IPAddress.Parse("255.255.255.255"));

        yield return new DataTypeSample("Enum('a' = 1, 'b' = 2)", typeof(string), "CAST('a', 'Enum(\\'a\\' = 1, \\'b\\' = 2)')", "a");
        yield return new DataTypeSample("Enum8('a' = -1, 'b' = 127)", typeof(string), "CAST('a', 'Enum8(\\'a\\' = -1, \\'b\\' = 127)')", "a");
        yield return new DataTypeSample("Enum16('a' = -32768, 'b' = 32767)", typeof(string), "CAST('a', 'Enum16(\\'a\\' = -32768, \\'b\\' = 32767)')", "a");

        yield return new DataTypeSample("Array(Int32)", typeof(int[]), "array(1, 2, 3)", new[] { 1, 2, 3 });
        yield return new DataTypeSample("Array(String)", typeof(int[]), "array('a', 'b', 'c')", new[] { "a", "b", "c" });
        yield return new DataTypeSample("Array(Nullable(Int32))", typeof(int?[]), "array(1, 2, NULL)", new int?[] { 1, 2, null });

        yield return new DataTypeSample("Nullable(Int32)", typeof(int?), "toInt32OrNull('123')", 123);
        yield return new DataTypeSample("Nullable(Int32)", typeof(int?), "toInt32OrNull(NULL)", DBNull.Value);
        yield return new DataTypeSample("Nullable(DateTime)", typeof(int?), "CAST(NULL AS Nullable(DateTime))", DBNull.Value);

        yield return new DataTypeSample("LowCardinality(Nullable(String))", typeof(string), "CAST(NULL AS LowCardinality(Nullable(String)))", DBNull.Value);
        yield return new DataTypeSample("LowCardinality(String)", typeof(string), "toLowCardinality('lowcardinality')", "lowcardinality");

        yield return new DataTypeSample("Tuple(Int8, String, Nullable(Int8))", typeof(Tuple<int, string, int?>), "tuple(1, 'a', 8)", Tuple.Create<int, string, int?>(1, "a", 8));
        yield return new DataTypeSample("Tuple(Int32, Tuple(UInt8, String, Nullable(Int32)))", typeof(Tuple<int, Tuple<byte, string, int?>>), "tuple(123, tuple(5, 'a', 7))", Tuple.Create(123, Tuple.Create((byte)5, "a", 7)));
        yield return new DataTypeSample("Tuple(a Int32, b Int32)", typeof(Tuple<int, ushort>), "tuple(123, 456)", Tuple.Create(123, 456));

        yield return new DataTypeSample("Date", typeof(DateTime), "toDateOrNull('1999-11-12')", new DateTime(1999, 11, 12, 0, 0, 0, DateTimeKind.Unspecified));
        yield return new DataTypeSample("DateTime('UTC')", typeof(DateTime), "toDateTime('1988-08-28 11:22:33', 'UTC')", new DateTime(1988, 08, 28, 11, 22, 33, DateTimeKind.Unspecified));
        yield return new DataTypeSample("DateTime('Pacific/Fiji')", typeof(DateTime), "toDateTime('1999-01-01 13:00:00', 'Pacific/Fiji')", new DateTime(1999, 01, 01, 13, 00, 00, DateTimeKind.Unspecified));

        yield return new DataTypeSample("DateTime64(4, 'UTC')", typeof(DateTime), "toDateTime64('2043-03-01 18:34:04.4444', 9, 'UTC')", new DateTime(644444444444444000, DateTimeKind.Utc));
        yield return new DataTypeSample("DateTime64(7, 'UTC')", typeof(DateTime), "toDateTime64('2043-03-01 18:34:04.4444444', 9, 'UTC')", new DateTime(644444444444444444, DateTimeKind.Utc));
        yield return new DataTypeSample("DateTime64(7, 'Pacific/Fiji')", typeof(DateTime), "toDateTime64('2043-03-01 18:34:04.4444444', 9, 'Pacific/Fiji')", new DateTime(644444444444444444, DateTimeKind.Unspecified));

        yield return new DataTypeSample("Decimal32(3)", typeof(ClickHouseDecimal), "toDecimal32(123.45, 3)", new ClickHouseDecimal(123.450m));
        yield return new DataTypeSample("Decimal32(3)", typeof(ClickHouseDecimal), "toDecimal32(-123.45, 3)", new ClickHouseDecimal(-123.450m));

        yield return new DataTypeSample("Decimal64(7)", typeof(ClickHouseDecimal), "toDecimal64(1.2345, 7)", new ClickHouseDecimal(1.2345000m));
        yield return new DataTypeSample("Decimal64(7)", typeof(ClickHouseDecimal), "toDecimal64(-1.2345, 7)", new ClickHouseDecimal(-1.2345000m));

        yield return new DataTypeSample("Decimal128(9)", typeof(ClickHouseDecimal), "toDecimal128(12.34, 9)", new ClickHouseDecimal(12.340000000m));
        yield return new DataTypeSample("Decimal128(9)", typeof(ClickHouseDecimal), "toDecimal128(-12.34, 9)", new ClickHouseDecimal(-12.340000000m));

        yield return new DataTypeSample("Decimal128(25)", typeof(ClickHouseDecimal), "toDecimal128(1e-24, 25)", new ClickHouseDecimal(10e-25m));
        yield return new DataTypeSample("Decimal128(0)", typeof(ClickHouseDecimal), "toDecimal128(repeat('1', 30), 0)", ClickHouseDecimal.Parse(new string('1', 30)));

        yield return new DataTypeSample("Decimal128(30)", typeof(ClickHouseDecimal), "toDecimal128(1, 30)", new ClickHouseDecimal(BigInteger.Pow(10, 30), 30));

        //yield return new DataTypeSample("Nested(Id int, Comment String)", typeof(Tuple<int, string>[]), "toNested(1, 'a')", new[] { Tuple.Create(1, "a") });

        if (SupportedFeatures.HasFlag(Feature.WideTypes))
        {
            // Code: 53. DB::Exception: Type mismatch in IN or VALUES section. Expected: Decimal(76, 25). Got: Decimal256:
            // While processing toDecimal256(1e-24, 25) AS expected, _CAST('0.000000000000000000000001', 'Decimal256(25)') AS actual, expected = actual AS equals. (TYPE_MISMATCH) (version 22.9.3.18 (official build))
            //yield return new DataTypeSample("Decimal256(25)", typeof(ClickHouseDecimal), "toDecimal256(1e-24, 25)", new ClickHouseDecimal(10e-25m));
            //yield return new DataTypeSample("Decimal256(0)", typeof(ClickHouseDecimal),
            //
            //"toDecimal256(repeat('1', 50), 0)", ClickHouseDecimal.Parse(new string('1', 50)));
            yield return new DataTypeSample("DateTime32('UTC')", typeof(DateTime), "toDateTime('1988-08-28 11:22:33', 'UTC')", new DateTime(1988, 08, 28, 11, 22, 33, DateTimeKind.Unspecified));
        }

        yield return new DataTypeSample("IPv6", typeof(IPAddress), "toIPv6('2001:0db8:85a3:0000:0000:8a2e:0370:7334')", IPAddress.Parse("2001:0db8:85a3:0000:0000:8a2e:0370:7334"));

        if (SupportedFeatures.HasFlag(Feature.Map))
        {
            yield return new DataTypeSample("Map(String, UInt8)", typeof(Dictionary<string, byte>), "map('A',1,'B',2)", new Dictionary<string, byte> { { "A", 1 }, { "B", 2 } });
            yield return new DataTypeSample("Map(UInt8, String)", typeof(Dictionary<byte, string>), "map(1,'A',2,'B')", new Dictionary<byte, string> { { 1, "A" }, { 2, "B" } });

            yield return new DataTypeSample("Map(String, Nullable(UInt8))",
                typeof(Dictionary<string, byte?>),
                "map('five', 5, 'null', NULL)",
                new Dictionary<string, byte?> {
                    { "five", 5 },
                    { "null", null },
                });
        }

        if (SupportedFeatures.HasFlag(Feature.Bool))
        {
            yield return new DataTypeSample("Bool", typeof(bool), "CAST(1, 'Bool')", true);
        }

        if (SupportedFeatures.HasFlag(Feature.Date32))
        {
            yield return new DataTypeSample("Date32", typeof(DateTime), "toDate32('2001-02-03')", new DateTime(2001, 02, 03));
            yield return new DataTypeSample("Date32", typeof(DateTime), "toDate32('1925-01-02')", new DateTime(1925, 01, 02));
        }

        if (SupportedFeatures.HasFlag(Feature.WideTypes))
        {
            yield return new DataTypeSample("Int128", typeof(BigInteger), "toInt128(concat('-1', repeat('0', 30)))", -BigInteger.Pow(new BigInteger(10), 30));
            yield return new DataTypeSample("Int128", typeof(BigInteger), "toInt128('170141183460469231731687303715884105727')", BigInteger.Parse("170141183460469231731687303715884105727"));
            yield return new DataTypeSample("Int128", typeof(BigInteger), "toInt128('-170141183460469231731687303715884105728')", BigInteger.Parse("-170141183460469231731687303715884105728"));

            yield return new DataTypeSample("UInt128", typeof(BigInteger), "toInt128(concat('1', repeat('0', 30)))", BigInteger.Pow(new BigInteger(10), 30));
            yield return new DataTypeSample("UInt128", typeof(BigInteger), "toUInt128('340282366920938463463374607431768211455')", BigInteger.Parse("340282366920938463463374607431768211455"));

            yield return new DataTypeSample("Int256", typeof(BigInteger), "toInt256(concat('-1', repeat('0', 50)))", -BigInteger.Pow(new BigInteger(10), 50));
            yield return new DataTypeSample("UInt256", typeof(BigInteger), "toInt256(concat('1', repeat('0', 50)))", BigInteger.Pow(new BigInteger(10), 50));
        }

        yield return new DataTypeSample("Point", typeof(Tuple<double, double>), "(10,20)", Tuple.Create(10.0, 20.0));
        yield return new DataTypeSample("Ring", typeof(Tuple<double, double>[]), "[(0.1,0.2), (0.2,0.3), (0.3,0.4)]", new[] {
            Tuple.Create(.1, .2),
            Tuple.Create(.2, .3),
            Tuple.Create(.3, .4)
        });

        if (SupportedFeatures.HasFlag(Feature.Variant))
        {
            yield return new DataTypeSample("Variant(UInt64, String, Array(UInt64))", typeof(string), "'Hello, World!'::Variant(UInt64, String, Array(UInt64))", "Hello, World!");
        }
    }

    public static object[] GetEnsureSingleRow(this DbDataReader reader)
    {
        Assert.IsTrue(reader.HasRows, "Reader expected to have rows");
        Assert.IsTrue(reader.Read(), "Failed to read first row");

        var data = reader.GetFieldValues();

        Assert.IsFalse(reader.Read(), "Unexpected extra row: " + string.Join(",", reader.GetFieldValues()));

        return data;
    }

    public static Type[] GetFieldTypes(this DbDataReader reader) => Enumerable.Range(0, reader.FieldCount).Select(reader.GetFieldType).ToArray();

    public static string[] GetFieldNames(this DbDataReader reader) => Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToArray();

    public static object[] GetFieldValues(this DbDataReader reader) => Enumerable.Range(0, reader.FieldCount).Select(reader.GetValue).ToArray();

    public static void AssertHasFieldCount(this DbDataReader reader, int expectedCount) => Assert.AreEqual(expectedCount, reader.FieldCount);
}
