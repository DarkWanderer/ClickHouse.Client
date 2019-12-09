using System;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Types
{
    internal class NullableTypeInfo : ParameterizedTypeInfo
    {
        public override ClickHouseTypeCode DataType => ClickHouseTypeCode.Nullable;

        public ClickHouseTypeInfo UnderlyingType { get; set; }

        public override Type EquivalentType => typeof(Nullable<>).MakeGenericType(UnderlyingType.EquivalentType);

        public override string Name => "Nullable";
        public override ParameterizedTypeInfo Parse(string typeName, Func<string, ClickHouseTypeInfo> typeResolverFunc)
        {
            if (!typeName.StartsWith(Name))
                throw new ArgumentException(nameof(typeName));

            return new NullableTypeInfo
            {
                UnderlyingType = typeResolverFunc(typeName.Substring(Name.Length).TrimRoundBrackets())
            };
        }

        public override string ToString() => $"Nullable({UnderlyingType.ToString()})";
    }
}
