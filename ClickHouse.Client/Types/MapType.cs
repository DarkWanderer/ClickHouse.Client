using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using ClickHouse.Client.Formats;
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

        public override object Read(ExtendedBinaryReader reader)
        {
            var dict = (IDictionary)Activator.CreateInstance(FrameworkType);

            var length = reader.Read7BitEncodedInt();

            for (var i = 0; i < length; i++)
            {
                var key = KeyType.Read(reader); // null is not supported as dictionary key in C#
                var value = ClearDBNull(ValueType.Read(reader));
                dict.Add(key, value);
            }
            return dict;
        }

        public override string ToString() => $"{Name}({keyType}, {valueType})";

        public override void Write(ExtendedBinaryWriter writer, object value)
        {
            var dict = (IDictionary)value;
            writer.Write7BitEncodedInt(dict.Count);
            foreach (DictionaryEntry kvp in dict)
            {
                KeyType.Write(writer, kvp.Key);
                ValueType.Write(writer, kvp.Value);
            }
        }
    }
}
