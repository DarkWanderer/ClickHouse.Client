using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using ClickHouse.Client.Formats;
using ClickHouse.Client.Types;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    [TestFixture]
    public class SerialisationTests
    {
        public static IEnumerable<TestCaseData> TestCases => TestUtilities.GetDataTypeSamples()
            .Select(sample => new TestCaseData(sample.ExampleValue, sample.ClickHouseType)
            { TestName = $"ShouldRoundtripSerialisation({sample.ExampleValue}, {sample.ClickHouseType})" });

        [Test]
        [TestCaseSource(nameof(TestCases))]
        public void ShouldRoundtripSerialisation(object written, string clickHouseType)
        {
            var type = TypeConverter.ParseClickHouseType(clickHouseType);

            using var stream = new MemoryStream();
            using var writer = new BinaryStreamWriter(new ExtendedBinaryWriter(stream));
            using var reader = new BinaryStreamReader(new ExtendedBinaryReader(stream));
            writer.Write(type, written);
            stream.Seek(0, SeekOrigin.Begin);
            var read = reader.Read(type);
            Assert.AreEqual(written, read, "Value read differs from value written");
            Assert.AreEqual(stream.Length, stream.Position, "Read underflow");
        }
    }
}
