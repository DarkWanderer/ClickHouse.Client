using System;
using System.Linq;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Types
{
    internal class NestedType : TupleType
    {
        public override ClickHouseTypeCode TypeCode => ClickHouseTypeCode.Nested;

        public override ParameterizedType Parse(string typeName, Func<string, ClickHouseType> typeResolverFunc)
        {
            if (!typeName.StartsWith(Name))
                throw new ArgumentException(nameof(typeName));

            var underlyingTypeNames = typeName.Substring(Name.Length).TrimRoundBrackets().Split(',');

            return new NestedType
            {
                UnderlyingTypes = underlyingTypeNames.Select(typeResolverFunc).ToArray()
            };
        }

        public override Type FrameworkType => base.FrameworkType.MakeArrayType();
    }
}
