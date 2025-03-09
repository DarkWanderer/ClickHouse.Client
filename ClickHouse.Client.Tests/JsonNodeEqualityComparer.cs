using System.Collections;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using NUnit.Framework.Constraints;

namespace ClickHouse.Client.Tests;

internal class JsonNodeEqualityComparer : IComparer<JsonObject>
{
    public int Compare(JsonObject x, JsonObject y) => JsonNode.DeepEquals(x, y) ? 0 : 1;
}
