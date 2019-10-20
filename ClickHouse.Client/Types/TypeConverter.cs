using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("ClickHouse.Client.Tests")] // assembly-level tag to expose below classes to tests

namespace ClickHouse.Client.Types
{
    internal static class TypeConverter
    {
        private static readonly IDictionary<ClickHouseDataType, TypeInfo> simpleTypes = new Dictionary<ClickHouseDataType, TypeInfo>();
        private static readonly IDictionary<Type, TypeInfo> reverseMapping = new Dictionary<Type, TypeInfo>();

        static TypeConverter()
        {

            // Unsigned integral types
            RegisterPlainTypeInfo<byte>(ClickHouseDataType.UInt8);
            RegisterPlainTypeInfo<ushort>(ClickHouseDataType.UInt16);
            RegisterPlainTypeInfo<uint>(ClickHouseDataType.UInt32);
            RegisterPlainTypeInfo<ulong>(ClickHouseDataType.UInt64);

            // Signed integral types
            RegisterPlainTypeInfo<sbyte>(ClickHouseDataType.Int8);
            RegisterPlainTypeInfo<short>(ClickHouseDataType.Int16);
            RegisterPlainTypeInfo<int>(ClickHouseDataType.Int32);
            RegisterPlainTypeInfo<long>(ClickHouseDataType.Int64);

            // Float types
            RegisterPlainTypeInfo<float>(ClickHouseDataType.Float32);
            RegisterPlainTypeInfo<double>(ClickHouseDataType.Float64);

            // String types
            RegisterPlainTypeInfo<string>(ClickHouseDataType.String);

            // Date/datetime mappings
            RegisterPlainTypeInfo<DateTime>(ClickHouseDataType.DateTime);
            RegisterPlainTypeInfo<DateTime>(ClickHouseDataType.Date);

            // Special 'nothing' type
            var nti = new NothingTypeInfo();
            simpleTypes.Add(ClickHouseDataType.Nothing, nti);
            reverseMapping.Add(typeof(DBNull), nti);

            // complex types like FixedString/Array/Nested etc. are handled separately because they have extended parameters
        }

        private static void RegisterPlainTypeInfo<T>(ClickHouseDataType type)
        {
            var typeInfo = new PlainDataTypeInfo<T>(type);
            simpleTypes.Add(type, typeInfo);
            if (!reverseMapping.ContainsKey(typeInfo.EquivalentType))
                reverseMapping.Add(typeInfo.EquivalentType, typeInfo);
        }

        private static bool TryParseComposite(string type, out string composite, out string underlyingType)
        {
            if (type.EndsWith(")", StringComparison.InvariantCulture) && type.Contains("(", StringComparison.InvariantCulture))
            {
                var split = type.Remove(type.Length - 1).Split('(', 2, StringSplitOptions.RemoveEmptyEntries);
                composite = split[0];
                underlyingType = split[1];
                return true;
            }
            composite = null;
            underlyingType = null;
            return false;
        }

        public static TypeInfo ParseClickHouseType(string type)
        {
            if (TryParseComposite(type, out var composite, out var underlyingType))
            {
                return composite switch
                {
                    "Nullable" => new NullableTypeInfo() { UnderlyingType = ParseClickHouseType(underlyingType) },
                    "Array" => new ArrayTypeInfo() { UnderlyingType = ParseClickHouseType(underlyingType) },
                    "FixedString" => new FixedStringTypeInfo { Length = int.Parse(underlyingType, CultureInfo.InvariantCulture) },
                    "DateTime" => DateTimeTypeInfo.ParseTimeZone(underlyingType),
                    _ => throw new ArgumentException("Unknown composite type: " + composite),
                };
            }
            if (Enum.TryParse<ClickHouseDataType>(type, out var chType) && simpleTypes.TryGetValue(chType, out var typeInfo))
                return typeInfo;
            throw new ArgumentOutOfRangeException(nameof(type), "Unknown type: " + type);
        }

        public static TypeInfo GetSimpleTypeInfo(string type)
        {
            if (Enum.TryParse<ClickHouseDataType>(type, out var chType) && simpleTypes.TryGetValue(chType, out var typeInfo))
                return typeInfo;
            throw new ArgumentOutOfRangeException(nameof(type), "Unknown type: " + type);
        }

        /// <summary>
        /// Recursively build ClickHouse type from .NET complex type
        /// Supports nullable and arrays
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public static TypeInfo ToClickHouseType(Type type)
        {
            if (type.IsArray)
                return new ArrayTypeInfo() { UnderlyingType = ToClickHouseType(type.GetElementType()) };
            var underlyingType = Nullable.GetUnderlyingType(type);
            if (underlyingType != null)
                return new NullableTypeInfo() { UnderlyingType = ToClickHouseType(underlyingType) };
            return reverseMapping[type];
        }
    }
}
