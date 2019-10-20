using System;
using System.Linq;

namespace ClickHouse.Client.Types
{
    internal class TupleTypeInfo : TypeInfo
    {
        public override ClickHouseDataType DataType => ClickHouseDataType.Tuple;

        public TypeInfo[] UnderlyingTypes { get; set; }

        public override Type EquivalentType => typeof(object[]);

        public override string ToString() => $"Tuple({string.Join(",", UnderlyingTypes.Select(t => t.ToString()))})";
    }
}
