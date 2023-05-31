using System;
using System.Collections;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using ClickHouse.Client.Formats;
using ClickHouse.Client.Types.Grammar;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Types;

internal class TupleType : ParameterizedType
{
    private Type frameworkType;
    private ClickHouseType[] underlyingTypes;

    public ClickHouseType[] UnderlyingTypes
    {
        get => underlyingTypes;
        set
        {
            underlyingTypes = value;
            frameworkType = DeviseFrameworkType(underlyingTypes);
        }
    }

    private static Type DeviseFrameworkType(ClickHouseType[] underlyingTypes)
    {
        var count = underlyingTypes.Length;

#if !NET462
        if (count > 7)
            return typeof(LargeTuple);
#endif

        var typeArgs = new Type[count];
        for (var i = 0; i < count; i++)
        {
            typeArgs[i] = underlyingTypes[i].FrameworkType;
        }
        var genericType = Type.GetType("System.Tuple`" + typeArgs.Length);
        return genericType.MakeGenericType(typeArgs);
    }

#if !NET462
    public ITuple MakeTuple(params object[] values)
    {
        var count = values.Length;
        if (underlyingTypes.Length != count)
            throw new ArgumentException($"Count of tuple type elements ({underlyingTypes.Length}) does not match number of elements ({count})");

        if (count > 7)
            return new LargeTuple(values);

        var valuesCopy = new object[count];

        // Coerce the values into types which can be stored in the tuple
        for (int i = 0; i < count; i++)
        {
            valuesCopy[i] = UnderlyingTypes[i].FrameworkType.IsSubclassOf(typeof(IConvertible)) ? Convert.ChangeType(values[i], UnderlyingTypes[i].FrameworkType, CultureInfo.InvariantCulture) : values[i];
        }

        return (ITuple)Activator.CreateInstance(frameworkType, valuesCopy);
    }
#endif

    public override Type FrameworkType => frameworkType;

    public override string Name => "Tuple";

    public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings)
    {
        return new TupleType
        {
            UnderlyingTypes = node.ChildNodes.Select(parseClickHouseTypeFunc).ToArray(),
        };
    }

    public override string ToString() => $"{Name}({string.Join(",", UnderlyingTypes.Select(t => t.ToString()))})";

    public override object Read(ExtendedBinaryReader reader)
    {
        var count = UnderlyingTypes.Length;
        var contents = new object[count];
        for (var i = 0; i < count; i++)
        {
            var value = UnderlyingTypes[i].Read(reader);
            contents[i] = ClearDBNull(value);
        }
#if !NET462
        return MakeTuple(contents);
#else
        return contents;
#endif
    }

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
#if !NET462
        if (value is ITuple tuple)
        {
            if (tuple.Length != UnderlyingTypes.Length)
                throw new ArgumentException("Wrong number of elements in Tuple", nameof(value));
            for (var i = 0; i < tuple.Length; i++)
            {
                UnderlyingTypes[i].Write(writer, tuple[i]);
            }
            return;
        }
#endif
        if (value is IList list)
        {
            if (list.Count != UnderlyingTypes.Length)
                throw new ArgumentException("Wrong number of elements in Tuple", nameof(value));
            for (var i = 0; i < list.Count; i++)
            {
                UnderlyingTypes[i].Write(writer, list[i]);
            }
            return;
        }
    }
}
