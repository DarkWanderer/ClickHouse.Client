using System.Collections.Generic;
using System.Linq;
using ClickHouse.Client.Types.Grammar;
using NUnit.Framework;

namespace ClickHouse.Client.Tests.Types;

public class TypeGrammarParsingTests
{
    [Test]
    [TestCaseSource(typeof(TypeGrammarParsingTests), nameof(Types))]
    public static void ShouldRoundTripParsedType(string input)
    {
        var output = Parser.Parse(input);
        Assert.That(output.ToString(), Is.EqualTo(input));
    }

    public static IList<string> Types => TestUtilities.GetDataTypeSamples().Select(s => s.ClickHouseType).Distinct().OrderBy(t => t).ToList();
}

