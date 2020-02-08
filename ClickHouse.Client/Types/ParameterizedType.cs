using System;

namespace ClickHouse.Client.Types
{
    internal abstract class ParameterizedType : ClickHouseType
    {
        public virtual string Name => TypeCode.ToString();

        public abstract ParameterizedType Parse(string typeName, Func<string, ClickHouseType> typeResolverFunc);
    }
}
