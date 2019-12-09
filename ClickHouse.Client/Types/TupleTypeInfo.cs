using System;
using System.Linq;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Types
{
    internal class TupleTypeInfo : ParameterizedTypeInfo
    {
        public override ClickHouseTypeCode DataType => ClickHouseTypeCode.Tuple;

        public ClickHouseTypeInfo[] UnderlyingTypes { get; set; }

        public override Type EquivalentType => typeof(Tuple<>).MakeGenericType(UnderlyingTypes.Select(t => t.EquivalentType).ToArray());

        public override string Name => "Tuple";

        public override ParameterizedTypeInfo Parse(string typeName, Func<string, ClickHouseTypeInfo> typeResolverFunc)
        {
            if (!typeName.StartsWith(Name))
                throw new ArgumentException(nameof(typeName));

            var underlyingTypeNames = typeName
                .Substring(Name.Length)
                .TrimRoundBrackets()
                .Split(',')
                .Select(t => t.Trim());

            return new TupleTypeInfo
            {
                UnderlyingTypes = underlyingTypeNames.Select(typeResolverFunc).ToArray()
            };
        }

        public override string ToString() => $"{Name}({string.Join(",", UnderlyingTypes.Select(t => t.ToString()))})";
    }

    internal class NestedTypeInfo : TupleTypeInfo
    {
        public override ClickHouseTypeCode DataType => ClickHouseTypeCode.Nested;

        public override ParameterizedTypeInfo Parse(string typeName, Func<string, ClickHouseTypeInfo> typeResolverFunc)
        {
            if (!typeName.StartsWith(Name))
                throw new ArgumentException(nameof(typeName));

            var underlyingTypeNames = typeName.Substring(Name.Length).TrimRoundBrackets().Split(',');

            return new NestedTypeInfo
            {
                UnderlyingTypes = underlyingTypeNames.Select(typeResolverFunc).ToArray()
            };
        }

        public override string Name => "Nested";
    }
}
