using System;
using System.Collections;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using ClickHouse.Client.ADO.Parameters;
using ClickHouse.Client.Types;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Formats
{
    public static class InlineParameterFormatter
    {
        public static string Format(ClickHouseDbParameter parameter)
        {
            if (parameter.Value is null)
                return string.Empty;
            var type = string.IsNullOrWhiteSpace(parameter.ClickHouseType)
                ? TypeConverter.ToClickHouseType(parameter.Value.GetType())
                : TypeConverter.ParseClickHouseType(parameter.ClickHouseType);
            return Format(type, parameter.Value);
        }

        internal static string Format(ClickHouseType type, object value)
        {
            return type.TypeCode switch
            {
                var simpleType when 
                    simpleType == ClickHouseTypeCode.UInt8 ||
                    simpleType == ClickHouseTypeCode.UInt16 ||
                    simpleType == ClickHouseTypeCode.UInt32 ||
                    simpleType == ClickHouseTypeCode.UInt64 ||
                    simpleType == ClickHouseTypeCode.Int8 ||
                    simpleType == ClickHouseTypeCode.Int16 ||
                    simpleType == ClickHouseTypeCode.Int32 ||
                    simpleType == ClickHouseTypeCode.Int64 => value.ToString(),
                
                var floatType when 
                    floatType == ClickHouseTypeCode.Float32 ||
                    floatType == ClickHouseTypeCode.Float64 => FormatFloat(value),
                
                ClickHouseTypeCode.Decimal => FormatDecimal(type, value),
                
                var stringType when
                    stringType == ClickHouseTypeCode.String ||
                    stringType == ClickHouseTypeCode.FixedString ||
                    stringType == ClickHouseTypeCode.LowCardinality ||
                    stringType == ClickHouseTypeCode.Enum8 ||
                    stringType == ClickHouseTypeCode.Enum16 ||
                    stringType == ClickHouseTypeCode.UUID=> value.ToString().Escape(),
                
                ClickHouseTypeCode.Nothing => $"null",

                ClickHouseTypeCode.Date when value is DateTime date => $"'{date:yyyy-MM-dd}'",
                ClickHouseTypeCode.DateTime when type is DateTimeType dateTimeType && value is DateTime dateTime =>
                    dateTimeType.TimeZone == null 
                        ? $"'{dateTime:yyyy-MM-dd HH:mm:ss}'" 
                        : $"'{dateTime.ToUniversalTime():yyyy-MM-dd HH:mm:ss}'",
                ClickHouseTypeCode.DateTime64 when type is DateTime64Type dateTimeType && value is DateTime dateTime =>
                    dateTimeType.TimeZone == null 
                        ? $"'{dateTime:yyyy-MM-dd HH:mm:ss.fff}'" 
                        : $"'{dateTime.ToUniversalTime():yyyy-MM-dd HH:mm:ss.fff}'",
                
                ClickHouseTypeCode.IPv4 when value is IPAddress iPAddressValue => $"toIPv4({iPAddressValue.ToString().Escape()})",
                ClickHouseTypeCode.IPv6 when value is IPAddress iPAddressValue => $"toIPv6({iPAddressValue.ToString().Escape()})",
                
                ClickHouseTypeCode.Nullable when type is NullableType nullableType => 
                    value is null || value == DBNull.Value ?
                        "null" :
                        $"{Format(nullableType.UnderlyingType, value)}",
                
                ClickHouseTypeCode.Array when type is ArrayType arrayType && value is IEnumerable enumerable =>
                    $"[{string.Join(',', enumerable.Cast<object>().Select(obj => Format(arrayType.UnderlyingType, obj)))}]",
                
                ClickHouseTypeCode.Tuple when type is TupleType tupleType && value is ITuple tuple =>
                $"({string.Join(',', tupleType.UnderlyingTypes.Select((x, i) => Format(x, tuple[i])))})",
                
                _ => throw new NotSupportedException($"Cannot convert value {value} to type {type.TypeCode}")
            };
        }

        private static string FormatFloat(object value)
        {
            return value switch
            {
                float floatValue => floatValue.ToString(CultureInfo.InvariantCulture),
                double doubleValue => doubleValue.ToString(CultureInfo.InvariantCulture),
                _ => throw new NotSupportedException($"Cannot convert value {value} to float type")
            };
        }

        private static string FormatDecimal(ClickHouseType type, object value)
        {
            if (!(value is decimal decimalValue))
                throw new NotSupportedException($"Cannot convert value {value} to decimal type");
            return type switch
            {
                Decimal128Type decimal128Type => $"toDecimal128({decimalValue.ToString(CultureInfo.InvariantCulture)},{decimal128Type.Scale})",
                Decimal64Type decimal64Type => $"toDecimal64({decimalValue.ToString(CultureInfo.InvariantCulture)},{decimal64Type.Scale})",
                Decimal32Type decimal32Type => $"toDecimal32({decimalValue.ToString(CultureInfo.InvariantCulture)},{decimal32Type.Scale})",
                DecimalType _ => decimalValue.ToString(CultureInfo.InvariantCulture),
                _ => throw new NotSupportedException($"Cannot convert value {value} to decimal type")
            };
        }
    }
}
