using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using ClickHouse.Client.Types.Grammar;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Types
{
    internal class ArrayType : ParameterizedType
    {
        public override ClickHouseTypeCode TypeCode => ClickHouseTypeCode.Array;

        public ClickHouseType UnderlyingType { get; set; }

        public override Type FrameworkType => UnderlyingType.FrameworkType.MakeArrayType();

        public override string Name => "Array";

        public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> typeResolverFunc)
        {
            return new ArrayType
            {
                UnderlyingType = typeResolverFunc(node.SingleChild),
            };
        }

        public Array MakeArray(int length) => Array.CreateInstance(UnderlyingType.FrameworkType, length);

        public override string ToStringParameter(object value) => !(value is IEnumerable<object> enumerable)
            ? throw new NotSupportedException($"Parameter value {value} is not a tuple")
            : $"({string.Join(',', enumerable.Select(UnderlyingType.ToStringParameter))})";

        public override string ToString() => $"Array({UnderlyingType.ToString()})";
    }
}
