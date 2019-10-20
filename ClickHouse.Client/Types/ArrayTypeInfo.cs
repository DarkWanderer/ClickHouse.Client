using System;
using System.Linq;

namespace ClickHouse.Client.Types
{
    internal class ArrayTypeInfo : TypeInfo
    {
        public override ClickHouseDataType DataType => ClickHouseDataType.Array;

        public TypeInfo UnderlyingType { get; set; }

        public override Type EquivalentType => UnderlyingType.EquivalentType.MakeArrayType();

        public override string ToString() => $"Array({UnderlyingType.ToString()})";
    }
}
