using System;
using System.Linq;
using ClickHouse.Client.Formats;
using ClickHouse.Client.Types.Grammar;

namespace ClickHouse.Client.Types;

internal class AggregateFunctionType : ParameterizedType
{
    public string Function { get; private set; }

    public override string Name => "AggregateFunction";



    public override Type FrameworkType => throw new AggregateFunctionException(Function);

    public override ParameterizedType Parse(SyntaxTreeNode typeName, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings)
    {
        return new AggregateFunctionType { Function = typeName.ChildNodes.First().Value };
    }

    public override object Read(ExtendedBinaryReader reader) => throw new AggregateFunctionException(Function);

    public override string ToString() => throw new AggregateFunctionException(Function);

    public override void Write(ExtendedBinaryWriter writer, object value) => throw new AggregateFunctionException(Function);

    [Serializable]
    public class AggregateFunctionException : Exception
    {
        public AggregateFunctionException(string function)
            : base($"Unable to directly query column with type AggregateFunction({function}). Use {function}Merge() function to query this value")
        {
        }
    }
}
