﻿namespace ClickHouse.Client.Types;

internal class PolygonType : ArrayType
{
    public PolygonType()
    {
        UnderlyingType = new RingType();
    }

    public override string ToString() => "Polygon";
}
