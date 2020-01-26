using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("ClickHouse.Client.Tests")] // assembly-level tag to expose below classes to tests

namespace ClickHouse.Client.Types
{
    internal static class TypeConverter
    {
        private static readonly IDictionary<ClickHouseTypeCode, ClickHouseType> simpleTypes = new Dictionary<ClickHouseTypeCode, ClickHouseType>();
        private static readonly IDictionary<string, ParameterizedType> parameterizedTypes = new Dictionary<string, ParameterizedType>();
        private static readonly IDictionary<Type, ClickHouseType> reverseMapping = new Dictionary<Type, ClickHouseType>();

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
            RegisterPlainTypeInfo<DateTime>(ClickHouseTypeCode.DateTime);
            RegisterPlainTypeInfo<DateTime>(ClickHouseTypeCode.Date);

            // Special 'nothing' type
            var nti = new NothingType();
            simpleTypes.Add(ClickHouseTypeCode.Nothing, nti);
            reverseMapping.Add(typeof(DBNull), nti);

            // complex types like FixedString/Array/Nested etc.
            RegisterParameterizedType<FixedStringType>();
            RegisterParameterizedType<ArrayType>();
            RegisterParameterizedType<NullableType>();
            RegisterParameterizedType<TupleType>();
            RegisterParameterizedType<NestedTypeInfo>();
            RegisterParameterizedType<LowCardinalityType>();

            RegisterParameterizedType<DateType>();
            RegisterParameterizedType<DateTimeType>();

            RegisterParameterizedType<DecimalType>();
            RegisterParameterizedType<Decimal32Type>();
            RegisterParameterizedType<Decimal64Type>();
            RegisterParameterizedType<Decimal128Type>();

            RegisterParameterizedType<EnumType>();
            RegisterParameterizedType<Enum8Type>();
            RegisterParameterizedType<Enum16Type>();

            reverseMapping.Add(typeof(decimal), new Decimal128Type());
        }

        private static void RegisterPlainTypeInfo<T>(ClickHouseTypeCode type)
        {
            var typeInfo = new PlainDataType<T>(type);
            simpleTypes.Add(type, typeInfo);
            if (!reverseMapping.ContainsKey(typeInfo.FrameworkType))
                reverseMapping.Add(typeInfo.FrameworkType, typeInfo);
        }

        private static void RegisterParameterizedType<T>() where T : ParameterizedType, new()
        {
            var t = new T();
            parameterizedTypes.Add(t.Name, t);
        }

        public static ClickHouseType ParseClickHouseType(string type)
        {
            if (Enum.TryParse<ClickHouseTypeCode>(type, out var chType) && simpleTypes.TryGetValue(chType, out var typeInfo))
                return typeInfo;
            var index = type.IndexOf('(');
            if (index > 0)
            {
                var parameterizedTypeName = type.Substring(0, index);
                if (parameterizedTypes.ContainsKey(parameterizedTypeName))
                    return parameterizedTypes[parameterizedTypeName].Parse(type, ParseClickHouseType);
            }
            throw new ArgumentOutOfRangeException(nameof(type), "Unknown type: " + type);
        }

        /// <summary>
        /// Recursively build ClickHouse type from .NET complex type
        /// Supports nullable and arrays
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public static ClickHouseType ToClickHouseType(Type type)
        {
            if (reverseMapping.ContainsKey(type))
                return reverseMapping[type];

            if (type.IsArray)
                return new ArrayType() { UnderlyingType = ToClickHouseType(type.GetElementType()) };

            var underlyingType = Nullable.GetUnderlyingType(type);
            if (underlyingType != null)
                return new NullableType() { UnderlyingType = ToClickHouseType(underlyingType) };

            if (type.IsGenericType && type.GetGenericTypeDefinition().FullName.StartsWith("System.Tuple"))
                return new TupleType { UnderlyingTypes = type.GetGenericArguments().Select(ToClickHouseType).ToArray() };

            throw new ArgumentOutOfRangeException(nameof(type), "Unknown type: " + type.ToString());
        }

        public static readonly DateTime DateTimeEpochStart = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    }
}
