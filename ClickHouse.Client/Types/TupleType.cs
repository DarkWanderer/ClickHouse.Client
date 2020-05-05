using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using ClickHouse.Client.Types.Grammar;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Types
{
    internal class TupleType : ParameterizedType
    {
        private Type frameworkType;
        private ClickHouseType[] underlyingTypes;

        public override ClickHouseTypeCode TypeCode => ClickHouseTypeCode.Tuple;

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
            var typeArgs = new Type[count];
            for (var i = 0; i < count; i++)
            {
                typeArgs[i] = underlyingTypes[i].FrameworkType;
            }

            var genericType = Type.GetType("System.Tuple`" + typeArgs.Length);
            return genericType.MakeGenericType(typeArgs);
        }

        public ITuple MakeTuple(params object[] values)
        {
            var count = values.Length;
            if (underlyingTypes.Length != count)
            {
                throw new ArgumentException($"Count of tuple type elements ({underlyingTypes.Length}) does not match number of elements ({count})");
            }

            var valuesCopy = new object[count];

            // Coerce the values into types which can be stored in the tuple
            for (int i = 0; i < count; i++)
            {
                if (values[i] is IConvertible convertible)
                    valuesCopy[i] = Convert.ChangeType(values[i], UnderlyingTypes[i].FrameworkType);
                else
                    valuesCopy[i] = values[i];
            }

            return (ITuple)Activator.CreateInstance(frameworkType, valuesCopy);
        }

        public override Type FrameworkType => frameworkType;

        public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> typeResolverFunc)
        {
            return new TupleType
            {
                UnderlyingTypes = node.ChildNodes.Select(typeResolverFunc).ToArray(),
            };
        }

        public override string ToHttpParameter(object value) => !(value is ITuple tuple)
            ? throw new NotSupportedException($"Parameter value {value} is not a tuple")
            : $"({string.Join(',', underlyingTypes.Select((x, i) => Uri.EscapeDataString(x.ToInlineParameter(tuple[i]))))})";
        
        public override string ToInlineParameter(object value) => !(value is ITuple tuple)
            ? throw new NotSupportedException($"Parameter value {value} is not a tuple")
            : $"({string.Join(',', underlyingTypes.Select((x, i) => x.ToInlineParameter(tuple[i])))})";

        public override string ToString() => $"{Name}({string.Join(",", UnderlyingTypes.Select(t => t.ToString()))})";
    }
}
