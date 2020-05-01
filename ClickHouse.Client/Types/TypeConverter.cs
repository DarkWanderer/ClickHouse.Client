using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using ClickHouse.Client.Types.Grammar;

[assembly: InternalsVisibleTo("ClickHouse.Client.Tests")] // assembly-level tag to expose below classes to tests

namespace ClickHouse.Client.Types
{
    internal static class TypeConverter
    {
        public static readonly DateTime DateTimeEpochStart = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        private static readonly IDictionary<ClickHouseTypeCode, ClickHouseType> SimpleTypes = new Dictionary<ClickHouseTypeCode, ClickHouseType>();
        private static readonly IDictionary<string, ParameterizedType> ParameterizedTypes = new Dictionary<string, ParameterizedType>();
        private static readonly IDictionary<Type, ClickHouseType> ReverseMapping = new Dictionary<Type, ClickHouseType>();

        static TypeConverter()
        {
            // Unsigned integral types
            RegisterPlainTypeInfo<byte>(ClickHouseTypeCode.UInt8);
            RegisterPlainTypeInfo<ushort>(ClickHouseTypeCode.UInt16);
            RegisterPlainTypeInfo<uint>(ClickHouseTypeCode.UInt32);
            RegisterPlainTypeInfo<ulong>(ClickHouseTypeCode.UInt64);

            // Signed integral types
            RegisterPlainTypeInfo<sbyte>(ClickHouseTypeCode.Int8);
            RegisterPlainTypeInfo<short>(ClickHouseTypeCode.Int16);
            RegisterPlainTypeInfo<int>(ClickHouseTypeCode.Int32);
            RegisterPlainTypeInfo<long>(ClickHouseTypeCode.Int64);

            // Float types
            RegisterPlainTypeInfo<float>(ClickHouseTypeCode.Float32);
            RegisterPlainTypeInfo<double>(ClickHouseTypeCode.Float64);

            // String types
            RegisterPlainTypeInfo<string>(ClickHouseTypeCode.String);

            RegisterPlainTypeInfo<Guid>(ClickHouseTypeCode.UUID);
            RegisterPlainTypeInfo<IPAddress>(ClickHouseTypeCode.IPv4);
            RegisterPlainTypeInfo<IPAddress>(ClickHouseTypeCode.IPv6);

            // Special 'nothing' type
            var nti = new NothingType();
            SimpleTypes.Add(ClickHouseTypeCode.Nothing, nti);
            ReverseMapping.Add(typeof(DBNull), nti);

            // complex types like FixedString/Array/Nested etc.
            RegisterParameterizedType<FixedStringType>();
            RegisterParameterizedType<ArrayType>();
            RegisterParameterizedType<NullableType>();
            RegisterParameterizedType<TupleType>();
            RegisterParameterizedType<NestedType>();
            RegisterParameterizedType<LowCardinalityType>();

            RegisterParameterizedType<DateType>();
            RegisterParameterizedType<DateTimeType>();
            RegisterParameterizedType<DateTime64Type>();

            RegisterParameterizedType<DecimalType>();
            RegisterParameterizedType<Decimal32Type>();
            RegisterParameterizedType<Decimal64Type>();
            RegisterParameterizedType<Decimal128Type>();

            RegisterParameterizedType<EnumType>();
            RegisterParameterizedType<Enum8Type>();
            RegisterParameterizedType<Enum16Type>();

            ReverseMapping.Add(typeof(decimal), new Decimal128Type());
            ReverseMapping.Add(typeof(DateTime), new DateTimeType());

            RegisterPlainTypeInfo<DateTime>(ClickHouseTypeCode.Date);
        }

        private static void RegisterPlainTypeInfo<T>(ClickHouseTypeCode type)
        {
            var typeInfo = new PlainDataType<T>(type);
            SimpleTypes.Add(type, typeInfo);
            if (!ReverseMapping.ContainsKey(typeInfo.FrameworkType))
            {
                ReverseMapping.Add(typeInfo.FrameworkType, typeInfo);
            }
        }

        private static void RegisterParameterizedType<T>()
            where T : ParameterizedType, new()
        {
            var t = new T();
            ParameterizedTypes.Add(t.Name, t);
        }

        public static ClickHouseType ParseClickHouseType(string type)
        {
            var node = Parser.Parse(type);
            return ParseClickHouseType(node);
        }

        public static ClickHouseType ParseClickHouseType(SyntaxTreeNode node)
        {
            if (
                node.ChildNodes.Count == 0 &&
                Enum.TryParse<ClickHouseTypeCode>(node.Value, out var chType) && 
                SimpleTypes.TryGetValue(chType, out var typeInfo))
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

            throw new ArgumentOutOfRangeException(nameof(type), "Unknown type: " + type.ToString());
        }
    }
}
