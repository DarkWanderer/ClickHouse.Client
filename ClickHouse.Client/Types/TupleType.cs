using System;
using System.Linq;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Types
{
    internal class TupleType : ParameterizedType
    {
        public override ClickHouseTypeCode DataType => ClickHouseTypeCode.Tuple;

        public ClickHouseType[] UnderlyingTypes { get; set; }

        public override Type EquivalentType => typeof(Tuple<>).MakeGenericType(UnderlyingTypes.Select(t => t.EquivalentType).ToArray());

        public override string Name => "Tuple";

        public override ParameterizedType Parse(string typeName, Func<string, ClickHouseType> typeResolverFunc)
        {
            if (!typeName.StartsWith(Name))
                throw new ArgumentException(nameof(typeName));

            var underlyingTypeNames = typeName
                .Substring(Name.Length)
                .TrimRoundBrackets()
                .Split(',')
                .Select(t => t.Trim());

            return new TupleType
            {
                UnderlyingTypes = underlyingTypeNames.Select(typeResolverFunc).ToArray()
            };
        }

        public override string ToString() => $"{Name}({string.Join(",", UnderlyingTypes.Select(t => t.ToString()))})";
    }

    internal class NestedTypeInfo : TupleType
    {
        public override ClickHouseTypeCode DataType => ClickHouseTypeCode.Nested;

        public override ParameterizedType Parse(string typeName, Func<string, ClickHouseType> typeResolverFunc)
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
