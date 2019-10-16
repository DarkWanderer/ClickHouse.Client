using System;
using System.Collections.Generic;
using System.Text;

namespace ClickHouse.Client.Types
{
    public static class TypeConverter
    {
        private static IDictionary<DataType, Type> directMapping = new Dictionary<DataType, Type>();
        private static IDictionary<Type, DataType> reverseMapping = new Dictionary<Type, DataType>();

        static TypeConverter()
        {
            // Bijective mappings
            // Unsigned integral types
            Add(typeof(byte), DataType.UInt8);
            Add(typeof(ushort), DataType.UInt16);
            Add(typeof(uint), DataType.UInt32);
            Add(typeof(ulong), DataType.UInt64);

            // Signed integral types
            Add(typeof(sbyte), DataType.Int8);
            Add(typeof(short), DataType.Int16);
            Add(typeof(int), DataType.Int32);
            Add(typeof(long), DataType.Int64);

            // Float types
            Add(typeof(float), DataType.Float32);
            Add(typeof(double), DataType.Float64);

            // String types
            Add(typeof(string), DataType.String);
            Add(typeof(DateTime), DataType.DateTime);

            // Non-bijective mappings
            directMapping.Add(DataType.Date, typeof(DateTime));
            directMapping.Add(DataType.FixedString, typeof(string));
        }

        private static void Add(Type type, DataType chType)
        {
            directMapping.Add(chType, type);
            reverseMapping.Add(type, chType);
        }

        private static bool TryExtractComposite(string type, out string composite, out string underlyingType)
        {
            if (type.EndsWith(")") && type.Contains("(")) 
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

        /// <summary>
        /// Recursively build .NET type from complex ClickHouse type
        /// /// Supports nullable and arrays
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
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
            if (Enum.TryParse<DataType>(type, out var chType))
                return FromClickHouseSimpleType(chType);
            throw new ArgumentException("Unknown type: " + type);
        }

        public static Type FromClickHouseSimpleType(DataType type) => directMapping[type];

        public static DataType GetClickHouseSimpleType(string type)
        {
            if (Enum.TryParse<DataType>(type, out var chType))
                return chType;
            else
                throw new ArgumentOutOfRangeException("type");
        }

        /// <summary>
        /// Recursively build ClickHouse type from .NET complex type
        /// Supports nullable and arrays
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public static string ToClickHouseType(Type type) {
            if (type.IsArray)
                return $"Array({ToClickHouseType(type.GetElementType())})";
            var underlyingType = Nullable.GetUnderlyingType(type);
            if (underlyingType != null)
                return $"Nullable({ToClickHouseType(underlyingType)})";
            return reverseMapping[type].ToString();
        }
    }
}
