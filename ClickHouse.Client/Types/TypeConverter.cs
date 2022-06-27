using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using ClickHouse.Client.Types.Grammar;

[assembly: InternalsVisibleTo("ClickHouse.Client.Tests")] // assembly-level tag to expose below classes to tests

namespace ClickHouse.Client.Types
{
    internal static class TypeConverter
    {
        private static readonly IDictionary<string, ClickHouseType> SimpleTypes = new Dictionary<string, ClickHouseType>();
        private static readonly IDictionary<string, ParameterizedType> ParameterizedTypes = new Dictionary<string, ParameterizedType>();
        private static readonly IDictionary<Type, ClickHouseType> ReverseMapping = new Dictionary<Type, ClickHouseType>();

        public static IEnumerable<string> RegisteredTypes => SimpleTypes.Keys
            .Concat(ParameterizedTypes.Values.Select(t => t.Name))
            .OrderBy(x => x)
            .ToArray();

        static TypeConverter()
        {
            RegisterPlainType<BooleanType>();

            // Integral types
            RegisterPlainType<Int8Type>();
            RegisterPlainType<Int16Type>();
            RegisterPlainType<Int32Type>();
            RegisterPlainType<Int64Type>();
            RegisterPlainType<Int128Type>();
            RegisterPlainType<Int256Type>();

            RegisterPlainType<UInt8Type>();
            RegisterPlainType<UInt16Type>();
            RegisterPlainType<UInt32Type>();
            RegisterPlainType<UInt64Type>();
            RegisterPlainType<UInt128Type>();
            RegisterPlainType<UInt256Type>();

            // Floating point types
            RegisterPlainType<Float32Type>();
            RegisterPlainType<Float64Type>();

            // Special types
            RegisterPlainType<UuidType>();
            RegisterPlainType<IPv4Type>();
            RegisterPlainType<IPv6Type>();

            // String types
            RegisterPlainType<StringType>();
            RegisterParameterizedType<FixedStringType>();

            // DateTime types
            RegisterPlainType<DateType>();
            RegisterPlainType<Date32Type>();
            RegisterParameterizedType<DateTimeType>();
            RegisterParameterizedType<DateTime64Type>();

            // Special 'nothing' type
            RegisterPlainType<NothingType>();

            // complex types like Tuple/Array/Nested etc.
            RegisterParameterizedType<ArrayType>();
            RegisterParameterizedType<NullableType>();
            RegisterParameterizedType<TupleType>();
            RegisterParameterizedType<NestedType>();
            RegisterParameterizedType<LowCardinalityType>();

            RegisterParameterizedType<DecimalType>();
            RegisterParameterizedType<Decimal32Type>();
            RegisterParameterizedType<Decimal64Type>();
            RegisterParameterizedType<Decimal128Type>();

            RegisterParameterizedType<EnumType>();
            RegisterParameterizedType<Enum8Type>();
            RegisterParameterizedType<Enum16Type>();
            RegisterParameterizedType<SimpleAggregateFunctionType>();
            RegisterParameterizedType<MapType>();

            ReverseMapping.Add(typeof(decimal), new Decimal128Type());
            ReverseMapping[typeof(DateTime)] = new DateTimeType();
            ReverseMapping[typeof(DateTimeOffset)] = new DateTimeType();
        }

        private static void RegisterPlainType<T>()
            where T : ClickHouseType, new()
        {
            var type = new T();
            var name = string.Intern(type.ToString()); // There is a limited number of types, interning them will help performance
            SimpleTypes.Add(name, type);
            if (!ReverseMapping.ContainsKey(type.FrameworkType))
            {
                ReverseMapping.Add(type.FrameworkType, type);
            }
        }

        private static void RegisterParameterizedType<T>()
            where T : ParameterizedType, new()
        {
            var t = new T();
            var name = string.Intern(t.Name); // There is a limited number of types, interning them will help performance
            ParameterizedTypes.Add(name, t);
        }

        public static ClickHouseType ParseClickHouseType(string type)
        {
            var node = Parser.Parse(type);
            return ParseClickHouseType(node);
        }

        internal static ClickHouseType ParseClickHouseType(SyntaxTreeNode node)
        {
            if (node.ChildNodes.Count == 0 && SimpleTypes.TryGetValue(node.Value, out var typeInfo))
            {
                return typeInfo;
            }

            if (ParameterizedTypes.ContainsKey(node.Value))
            {
                return ParameterizedTypes[node.Value].Parse(node, ParseClickHouseType);
            }

            throw new ArgumentException("Unknown type: " + node.ToString());
        }

        /// <summary>
        /// Recursively build ClickHouse type from .NET complex type
        /// Supports nullable and arrays.
        /// </summary>
        /// <param name="type">framework type to map</param>
        /// <returns>Corresponding ClickHouse type</returns>
        public static ClickHouseType ToClickHouseType(Type type)
        {
            if (ReverseMapping.ContainsKey(type))
            {
                return ReverseMapping[type];
            }

            if (type.IsArray)
            {
                return new ArrayType() { UnderlyingType = ToClickHouseType(type.GetElementType()) };
            }

            var underlyingType = Nullable.GetUnderlyingType(type);
            if (underlyingType != null)
            {
                return new NullableType() { UnderlyingType = ToClickHouseType(underlyingType) };
            }

            if (type.IsGenericType && type.GetGenericTypeDefinition().FullName.StartsWith("System.Tuple"))
            {
                return new TupleType { UnderlyingTypes = type.GetGenericArguments().Select(ToClickHouseType).ToArray() };
            }

            if (type.IsGenericType && type.GetGenericTypeDefinition().FullName.StartsWith("System.Collections.Generic.Dictionary"))
            {
                var types = type.GetGenericArguments().Select(ToClickHouseType).ToArray();
                return new MapType { UnderlyingTypes = Tuple.Create(types[0], types[1]) };
            }

            throw new ArgumentOutOfRangeException(nameof(type), "Unknown type: " + type.ToString());
        }
    }
}
