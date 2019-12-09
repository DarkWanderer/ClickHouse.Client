using System;

namespace ClickHouse.Client.Types
{
    internal abstract class ParameterizedType : ClickHouseType
    {
        public abstract string Name { get; }

        public abstract ParameterizedType Parse(string typeName, Func<string, ClickHouseType> typeResolverFunc);
    }
}
