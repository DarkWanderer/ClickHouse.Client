using System;
using System.Linq;
using System.Runtime.Serialization;
using System.Xml.Linq;
using ClickHouse.Client.Formats;
using ClickHouse.Client.Types.Grammar;
using NodaTime;

namespace ClickHouse.Client.Types;

internal abstract class ParameterizedType : ClickHouseType
{
    public abstract string Name { get; }

    public abstract ParameterizedType Parse(SyntaxTreeNode typeName, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings);
}
