using System;
using ClickHouse.Client.Types.Grammar;

namespace ClickHouse.Client.Types;

internal abstract class ParameterizedType : ClickHouseType
{
    public abstract string Name { get; }

    public abstract ParameterizedType Parse(SyntaxTreeNode typeName, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings);
}
