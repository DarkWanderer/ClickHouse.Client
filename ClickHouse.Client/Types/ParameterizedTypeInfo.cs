using System;

namespace ClickHouse.Client.Types
{
    internal abstract class ParameterizedTypeInfo : ClickHouseTypeInfo
    {
        public abstract string Name { get; }

        public abstract ParameterizedTypeInfo Parse(string typeName, Func<string, ClickHouseTypeInfo> typeResolverFunc);
    }
}
