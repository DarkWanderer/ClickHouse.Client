using System;
using System.Collections.Generic;
using System.Text;
using ClickHouse.Client.Types.Grammar;
using NUnit.Framework;

namespace ClickHouse.Client.Tests.Syntax
{
    public class TypeGrammarParsingTests
    {
        [Test]
        [TestCase("Int32")]
        [TestCase("Nested(Int32, String, Tuple(UInt8, FixedString(3)))")]
        [TestCase("Tuple(Int32, Tuple(UInt8, String, Nullable(Int32)))")]
        public void ShouldRoundTripParsedType(string input)
        {
            var output = Parser.Parse(input);
            Assert.AreEqual(input, output.ToString());
        }
    }
}
