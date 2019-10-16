using System;
using System.Collections.Generic;
using System.Text;

namespace ClickHouse.Client
{
    public static class ClickHouseTypeConverter
    {
        private static IDictionary<ClickHouseDataType, Type> directMapping = new Dictionary<ClickHouseDataType, Type>();
        private static IDictionary<Type, ClickHouseDataType> reverseMapping = new Dictionary<Type, ClickHouseDataType>();

        static ClickHouseTypeConverter()
        {
            // Bijective mappings
            Add(typeof(ushort), ClickHouseDataType.UInt16);
            Add(typeof(uint), ClickHouseDataType.UInt32);
            Add(typeof(ulong), ClickHouseDataType.UInt64);
            Add(typeof(short), ClickHouseDataType.Int16);
            Add(typeof(int), ClickHouseDataType.Int32);
            Add(typeof(long), ClickHouseDataType.Int64);

            Add(typeof(float), ClickHouseDataType.Float32);
            Add(typeof(double), ClickHouseDataType.Float64);

            Add(typeof(string), ClickHouseDataType.String);
            Add(typeof(DateTime), ClickHouseDataType.DateTime);

            // Non-bijective mappings
            directMapping.Add(ClickHouseDataType.UInt8, typeof(ushort));
            directMapping.Add(ClickHouseDataType.Int8, typeof(short));
            directMapping.Add(ClickHouseDataType.Date, typeof(DateTime));
            directMapping.Add(ClickHouseDataType.FixedString, typeof(string));
        }

        private static void Add(Type type, ClickHouseDataType chType)
        {
            directMapping.Add(chType, type);
            reverseMapping.Add(type, chType);
        }

        private static bool TryExtractComposite(string type, out string composite, out string underlyingType)
        {
            if (type.EndsWith(")") && type.Contains("(")) {
                var split = type.TrimEnd(')').Split('(', 1, StringSplitOptions.RemoveEmptyEntries);
                composite = split[0];
                underlyingType = split[1];
                return true;
            }
            composite = null;
            underlyingType = null;
            return false;
        }

        public static Type FromClickHouseType(string type)
        {
            if (TryExtractComposite(type, out string composite, out string underlyingType))
            {
                return composite switch
                {
                    "Nullable" => typeof(Nullable<>).MakeGenericType(FromClickHouseType(underlyingType)),
                    "Array" => FromClickHouseType(underlyingType).MakeArrayType(),
                    _ => throw new ArgumentException("Unknown composite type: " + composite),
                };
            }
            if (Enum.TryParse<ClickHouseDataType>(type, out var chType))
                return FromClickHouseType(chType);
            throw new ArgumentException("Unknown type: " + type);
        }

        public static Type FromClickHouseType(ClickHouseDataType type) => directMapping[type];

        public static ClickHouseDataType ToClickHouseType(Type type) => reverseMapping[type];
    }
}
