using System;
using System.Collections.Generic;
using System.Linq;
using ClickHouse.Client.Types.Grammar;

namespace ClickHouse.Client.Types
{
    internal class MapType : ParameterizedType
    {
        private Type frameworkType;
        private ClickHouseType keyType;
        private ClickHouseType valueType;

        public Tuple<ClickHouseType, ClickHouseType> UnderlyingTypes
        {
            get => Tuple.Create(keyType, valueType);

            set
            {
                keyType = value.Item1;
                valueType = value.Item2;

                var genericType = typeof(Dictionary<,>);
                frameworkType = genericType.MakeGenericType(new[] { keyType.FrameworkType, valueType.FrameworkType });
            }
        }

        public ClickHouseType KeyType => keyType;

        public ClickHouseType ValueType => valueType;

        public override Type FrameworkType => frameworkType;

        public override string Name => "Map";

        public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc)
        {
            var types = node.ChildNodes.Select(parseClickHouseTypeFunc).ToArray();
            var result = new MapType() { UnderlyingTypes = Tuple.Create(types[0], types[1]) };
            return result;
        }

        public override string ToString() => $"{Name}({keyType}, {valueType})";

        public override object AcceptRead(ISerializationTypeVisitorReader reader) => reader.Read(this);

        public override void AcceptWrite(ISerializationTypeVisitorWriter writer, object value) => writer.Write(this, value);
    }
}
