using System.Text.Json.Nodes;
using NUnit.Framework.Constraints;

namespace ClickHouse.Client.Tests;

internal class JsonNodeEqualityComparer : EqualityAdapter
{
    public override bool CanCompare(object x, object y) => x is JsonNode && y is JsonNode;

    public override bool AreEqual(object x, object y) => x.ToString() == y.ToString();
}
