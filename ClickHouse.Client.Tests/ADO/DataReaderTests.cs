using System.Data;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Client.ADO.Readers;
using ClickHouse.Client.Utility;
using NUnit.Framework;

namespace ClickHouse.Client.Tests.ADO;

public class DataReaderTests : AbstractConnectionTestFixture
{
    [Test]
    public async Task ShouldReadFieldByIndex()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT 1 as value");
        Assert.IsTrue(reader.Read());
        Assert.AreEqual(1, reader[0]);
        Assert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadFieldByName()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT 1 as value");
        Assert.IsTrue(reader.Read());
        Assert.AreEqual(1, reader["value"]);
        Assert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadBoolean()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT 1 as value");
        Assert.IsTrue(reader.Read());
        Assert.AreEqual(true, reader.GetBoolean(0));
        Assert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadByte()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT toUInt8(1) as value");
        Assert.IsTrue(reader.Read());
        Assert.AreEqual(1, reader.GetByte(0));
        Assert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadFloat()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT toFloat32(1) as value");
        Assert.IsTrue(reader.Read());
        Assert.AreEqual(1, reader.GetFloat(0));
        Assert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadDouble()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT toFloat64(1) as value");
        Assert.IsTrue(reader.Read());
        Assert.AreEqual(1, reader.GetDouble(0));
        Assert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadInt16()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT toInt16(1) as value");
        Assert.IsTrue(reader.Read());
        Assert.AreEqual(1, reader.GetInt16(0));
        Assert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadInt32()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT toInt32(1) as value");
        Assert.IsTrue(reader.Read());
        Assert.AreEqual(1, reader.GetInt32(0));
        Assert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadInt64()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT toInt64(1) as value");
        Assert.IsTrue(reader.Read());
        Assert.AreEqual(1, reader.GetInt64(0));
        Assert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadUInt16()
    {
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync("SELECT toUInt16(1) as value");
        Assert.IsTrue(reader.Read());
        Assert.AreEqual(1, reader.GetUInt16(0));
        Assert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadUInt32()
    {
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync("SELECT toUInt32(1) as value");
        Assert.IsTrue(reader.Read());
        Assert.AreEqual(1, reader.GetUInt32(0));
        Assert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadUInt64()
    {
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync("SELECT toUInt64(1) as value");
        Assert.IsTrue(reader.Read());
        Assert.AreEqual(1, reader.GetUInt64(0));
        Assert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadDecimal()
    {
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync("SELECT toDecimal64(1,3) as value");
        Assert.IsTrue(reader.Read());
        Assert.AreEqual(1.000m, reader.GetDecimal(0));
        Assert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadString()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT 'ASD' as value");
        Assert.IsTrue(reader.Read());
        Assert.AreEqual("ASD", reader.GetString(0));
        Assert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadNull()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT NULL as value");
        Assert.IsTrue(reader.Read());
        Assert.IsTrue(reader.IsDBNull(0));
        Assert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadIPv4()
    {
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync("SELECT toIPv4('1.2.3.4')");
        Assert.IsTrue(reader.Read());
        Assert.IsNotNull(reader.GetIPAddress(0));
        Assert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadTuple()
    {
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync("SELECT tuple(1,'a', NULL)");
        Assert.IsTrue(reader.Read());
        Assert.IsNotNull(reader.GetTuple(0));
        Assert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadGuid()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT generateUUIDv4() as value");
        Assert.IsTrue(reader.Read());
        Assert.IsNotNull(reader.GetGuid(0));
        Assert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldGetFieldValue()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT 'ASD' as value");
        Assert.IsTrue(reader.Read());
        Assert.AreEqual("ASD", reader.GetFieldValue<string>(0));
        Assert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldGetDataTypeName()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT 'ASD' as value");
        Assert.IsTrue(reader.Read());
        Assert.AreEqual("String", reader.GetDataTypeName(0));
        Assert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldEnumerateRows()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT * FROM system.numbers LIMIT 100");
        var rows = reader.Cast<IDataRecord>().Select(row => row[0]).ToList();
        CollectionAssert.AreEqual(Enumerable.Range(0, 100), rows);
        Assert.IsFalse(reader.Read());
    }
}
