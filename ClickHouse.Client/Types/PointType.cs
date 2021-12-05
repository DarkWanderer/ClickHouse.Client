using System;
using ClickHouse.Client.Formats;

namespace ClickHouse.Client.Types
{
    internal class PointType : TupleType
    {
        public PointType()
        {
            UnderlyingTypes = new[] { new Float64Type(), new Float64Type() };
        }

        public override string ToString() => "Point";
    }
}
