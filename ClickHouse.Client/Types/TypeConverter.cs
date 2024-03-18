using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using ClickHouse.Client.Numerics;
using ClickHouse.Client.Types.Grammar;

[assembly: InternalsVisibleTo("ClickHouse.Client.Tests")] // assembly-level tag to expose below classes to tests

namespace ClickHouse.Client.Types;

internal static class TypeConverter
{
    private static readonly IDictionary<string, ClickHouseType> SimpleTypes = new Dictionary<string, ClickHouseType>();
    private static readonly IDictionary<string, ParameterizedType> ParameterizedTypes = new Dictionary<string, ParameterizedType>();
    private static readonly IDictionary<Type, ClickHouseType> ReverseMapping = new Dictionary<Type, ClickHouseType>();

    private static readonly IDictionary<string, string> Aliases = new Dictionary<string, string>()
    {
        { "BIGINT", "Int64" },
        { "BIGINT SIGNED", "Int64" },
        { "BIGINT UNSIGNED", "UInt64" },
        { "BINARY", "FixedString" },
        { "BINARY LARGE OBJECT", "String" },
        { "BINARY VARYING", "String" },
        { "BIT", "UInt64" },
        { "BLOB", "String" },
        { "BYTE", "Int8" },
        { "BYTEA", "String" },
        { "CHAR", "String" },
        { "CHAR LARGE OBJECT", "String" },
        { "CHAR VARYING", "String" },
        { "CHARACTER", "String" },
        { "CHARACTER LARGE OBJECT", "String" },
        { "CHARACTER VARYING", "String" },
        { "CLOB", "String" },
        { "DEC", "Decimal" },
        { "DOUBLE", "Float64" },
        { "DOUBLE PRECISION", "Float64" },
        { "ENUM", "Enum" },
        { "FIXED", "Decimal" },
        { "FLOAT", "Float32" },
        { "GEOMETRY", "String" },
        { "INET4", "IPv4" },
        { "INET6", "IPv6" },
        { "INT", "Int32" },
        { "INT SIGNED", "Int32" },
        { "INT UNSIGNED", "UInt32" },
        { "INT1", "Int8" },
        { "INT1 SIGNED", "Int8" },
        { "INT1 UNSIGNED", "UInt8" },
        { "INTEGER", "Int32" },
        { "INTEGER SIGNED", "Int32" },
        { "INTEGER UNSIGNED", "UInt32" },
        { "LONGBLOB", "String" },
        { "LONGTEXT", "String" },
        { "MEDIUMBLOB", "String" },
        { "MEDIUMINT", "Int32" },
        { "MEDIUMINT SIGNED", "Int32" },
        { "MEDIUMINT UNSIGNED", "UInt32" },
        { "MEDIUMTEXT", "String" },
        { "NATIONAL CHAR", "String" },
        { "NATIONAL CHAR VARYING", "String" },
        { "NATIONAL CHARACTER", "String" },
        { "NATIONAL CHARACTER LARGE OBJECT", "String" },
        { "NATIONAL CHARACTER VARYING", "String" },
        { "NCHAR", "String" },
        { "NCHAR LARGE OBJECT", "String" },
        { "NCHAR VARYING", "String" },
        { "NUMERIC", "Decimal" },
        { "NVARCHAR", "String" },
        { "REAL", "Float32" },
        { "SET", "UInt64" },
        { "SINGLE", "Float32" },
        { "SMALLINT", "Int16" },
        { "SMALLINT SIGNED", "Int16" },
        { "SMALLINT UNSIGNED", "UInt16" },
        { "TEXT", "String" },
        { "TIME", "Int64" },
        { "TIMESTAMP", "DateTime" },
        { "TINYBLOB", "String" },
        { "TINYINT", "Int8" },
        { "TINYINT SIGNED", "Int8" },
        { "TINYINT UNSIGNED", "UInt8" },
        { "TINYTEXT", "String" },
        { "VARBINARY", "String" },
        { "VARCHAR", "String" },
        { "VARCHAR2", "String" },
        { "YEAR", "UInt16" },
        { "BOOL", "Bool" },
        { "BOOLEAN", "Bool" },
        { "OBJECT('JSON')", "Json" },
        { "JSON", "Json" },
    };

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
        RegisterParameterizedType<DateTime32Type>();
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
        RegisterParameterizedType<Decimal256Type>();

        RegisterParameterizedType<EnumType>();
        RegisterParameterizedType<Enum8Type>();
        RegisterParameterizedType<Enum16Type>();
        RegisterParameterizedType<SimpleAggregateFunctionType>();
        RegisterParameterizedType<MapType>();
        RegisterParameterizedType<VariantType>();

        // Geo types
        RegisterPlainType<PointType>();
        RegisterPlainType<RingType>();
        RegisterPlainType<PolygonType>();
        RegisterPlainType<MultiPolygonType>();

        // JSON/Object
        RegisterParameterizedType<ObjectType>();

        // Mapping fixups
        ReverseMapping.Add(typeof(ClickHouseDecimal), new Decimal128Type());
        ReverseMapping.Add(typeof(decimal), new Decimal128Type());
#if NET6_0_OR_GREATER
        ReverseMapping.Add(typeof(DateOnly), new DateType());
#endif
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

    public static ClickHouseType ParseClickHouseType(string type, TypeSettings settings)
    {
        var node = Parser.Parse(type);
        return ParseClickHouseType(node, settings);
    }

    internal static ClickHouseType ParseClickHouseType(SyntaxTreeNode node, TypeSettings settings)
    {
        var typeName = node.Value.Trim().Trim('\'');

        if (Aliases.TryGetValue(typeName.ToUpperInvariant(), out var alias))
            typeName = alias;

        if (typeName.Contains(' '))
        {
            var parts = typeName.Split(new[] { " " }, 2, StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length == 2)
            {
                typeName = parts[1].Trim();
            }
            else
            {
                throw new ArgumentException($"Cannot parse {node.Value} as type", nameof(node));
            }
        }

        if (node.ChildNodes.Count == 0 && SimpleTypes.TryGetValue(typeName, out var typeInfo))
        {
            return typeInfo;
        }

        if (ParameterizedTypes.ContainsKey(typeName))
        {
            return ParameterizedTypes[typeName].Parse(node, (n) => ParseClickHouseType(n, settings), settings);
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

        if (type.IsGenericType && type.GetGenericTypeDefinition().FullName.StartsWith("System.Tuple", StringComparison.InvariantCulture))
        {
            return new TupleType { UnderlyingTypes = type.GetGenericArguments().Select(ToClickHouseType).ToArray() };
        }

        if (type.IsGenericType && type.GetGenericTypeDefinition().FullName.StartsWith("System.Collections.Generic.Dictionary", StringComparison.InvariantCulture))
        {
            var types = type.GetGenericArguments().Select(ToClickHouseType).ToArray();
            return new MapType { UnderlyingTypes = Tuple.Create(types[0], types[1]) };
        }

        throw new ArgumentOutOfRangeException(nameof(type), "Unknown type: " + type.ToString());
    }
}
