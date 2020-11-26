using System.Threading.Tasks;
using NUnit.Framework;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Tests
{
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
        public async Task ShouldEnumerateCurrentRowValues()
        {
            using var reader = await connection.ExecuteReaderAsync("SELECT 1,2,3");
            Assert.IsTrue(reader.Read());
            CollectionAssert.AreEqual(new[] { 1, 2, 3 }, reader);
            Assert.IsFalse(reader.Read());
        }
    }
}
