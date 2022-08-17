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

        public override void Write(ExtendedBinaryWriter writer, object value)
        {
            if (value is System.Drawing.Point p)
                value = Tuple.Create(p.X, p.Y);
            base.Write(writer, value);
        }

        public override string ToString() => "Point";
    }
}
