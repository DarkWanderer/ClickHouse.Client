using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using ClickHouse.Client.Formats;
using ClickHouse.Client.Types;

namespace ClickHouse.Client.Tests.Misc;

[TestFixture]
public class SerialisationTests
{
    public static IEnumerable<TestCaseData> TestCases => TestUtilities.GetDataTypeSamples()
        .Select(sample => new TestCaseData(sample.ExampleValue, sample.ClickHouseType)
        { TestName = $"ShouldRoundtripSerialisation({sample.ExampleExpression}, {sample.ClickHouseType})" });

    [Test]
    [TestCaseSource(nameof(TestCases))]
    public void ShouldRoundtripSerialisation(object expected, string clickHouseType)
    {
        var type = TypeConverter.ParseClickHouseType(clickHouseType, TypeSettings.Default);

        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);
        using var reader = new ExtendedBinaryReader(stream);
        type.Write(writer, expected);

        var data = Convert.ToHexString(stream.ToArray());
        stream.Seek(0, SeekOrigin.Begin);

        var actual = type.Read(reader);
        Assert.That(actual, Is.EqualTo(expected).UsingPropertiesComparer(), "Different value read from stream");

        ClassicAssert.AreEqual(stream.Length, stream.Position, "Read underflow");
    }

    [Test]
    public void BinaryReaderShouldThrowOnOverflow()
    {
        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);
        using var reader = new ExtendedBinaryReader(stream);

        writer.Write((short)1);
        stream.Seek(0, SeekOrigin.Begin);
        Assert.Throws<EndOfStreamException>(() => reader.ReadInt64());
    }
}
