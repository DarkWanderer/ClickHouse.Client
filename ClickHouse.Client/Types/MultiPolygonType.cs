using System;
using ClickHouse.Client.Formats;

namespace ClickHouse.Client.Types;

internal class MultiPolygonType : ArrayType
{
    public MultiPolygonType()
    {
        UnderlyingType = new PolygonType();
    }

    public override string ToString() => "MultiPolygon";
}
