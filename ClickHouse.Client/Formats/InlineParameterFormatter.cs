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
            var type = string.IsNullOrWhiteSpace(parameter.ClickHouseType)
                ? TypeConverter.ToClickHouseType(parameter.Value.GetType())
                : TypeConverter.ParseClickHouseType(parameter.ClickHouseType);
            return Format(type, parameter.Value);
        }

        internal static string Format(ClickHouseType type, object value)
        {
            switch (type)
            {
                case IntegerType it:
                    return Convert.ToString(value, CultureInfo.InvariantCulture);

                case FloatType ft:
                    return FormatFloat(value);

                case DecimalType dt:
                    return FormatDecimal(dt, value);

                case LowCardinalityType lt:
                    return Format(lt.UnderlyingType, value);

                case StringType st:
                case FixedStringType tt:
                case Enum8Type e8t:
                case Enum16Type e16t:
                    return value.ToString().Escape();

                case UuidType ut:
                    return $"toUUID({value.ToString().Escape()})";

                case NothingType nt:
                    return "NULL";

                case DateType dt when value is DateTime dtv:
                    return $"toDate('{dtv:yyyy-MM-dd}')";

                case DateTimeType dtt when value is DateTime dtv:
                    return dtt.TimeZone == null
                        ? $"toDateTime('{dtv:yyyy-MM-dd HH:mm:ss}')"
                        : $"toDateTime('{dtv:yyyy-MM-dd HH:mm:ss}', '{dtt.TimeZone.Id}')";

                case DateTimeType dtt when value is DateTimeOffset dto:
                    return dtt.TimeZone == null
                        ? $"toDateTime('{dto:yyyy-MM-dd HH:mm:ss}')"
                        : $"toDateTime('{dto:yyyy-MM-dd HH:mm:ss}', '{dtt.TimeZone.Id}')";

                case DateTime64Type dtt when value is DateTime dtv:
                    var @string = dtt.ToZonedDateTime(dtv).ToString("yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
                    return dtt.TimeZone != null
                        ? $"toDateTime64('{@string}', 7, '{dtt.TimeZone}')"
                        : $"toDateTime64('{@string}', 7)";

                case DateTime64Type dtt when value is DateTimeOffset dto:
                    var @str2 = dtt.ToZonedDateTime(dto.DateTime).ToString("yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
                    return dtt.TimeZone != null
                        ? $"toDateTime64('{@str2}', 7, '{dtt.TimeZone}')"
                        : $"toDateTime64('{@str2}', 7)";

                case IPv4Type it: return $"toIPv4({FormatIPAddress(value)})";
                case IPv6Type it: return $"toIPv6({FormatIPAddress(value)})";

                case NullableType nullableType:
                    return value is null || value is DBNull ? "null" : $"{Format(nullableType.UnderlyingType, value)}";

                case ArrayType arrayType when value is IEnumerable enumerable:
                    var array = enumerable.Cast<object>().Select(obj => Format(arrayType.UnderlyingType, obj));
                    return $"[{string.Join(",", array)}]";

                case TupleType tupleType when value is ITuple tuple:
                    return $"({string.Join(",", tupleType.UnderlyingTypes.Select((x, i) => Format(x, tuple[i])))})";

                case MapType mapType when value is IDictionary dict:
                    var strings = dict.Keys.Cast<object>().Select(k => $"{Format(mapType.KeyType, k)},{Format(mapType.ValueType, dict[k])}");
                    return $"map({string.Join(",", strings)})";

                default:
                    throw new NotSupportedException($"Cannot convert value {value} to ClickHouse type {type}");
            }
        }

        private static object FormatIPAddress(object value) => value switch
        {
            IPAddress ipAddress => ipAddress.ToString().Escape(),
            string str => str,
            _ => throw new NotSupportedException($"Cannot convert value {value} to IP address")
        };

        private static string FormatFloat(object value) => value switch
        {
            float floatValue => floatValue.ToString(CultureInfo.InvariantCulture),
            double doubleValue => doubleValue.ToString(CultureInfo.InvariantCulture),
            _ => Convert.ToDouble(value).ToString(CultureInfo.InvariantCulture)
        };

        private static string FormatDecimal(DecimalType type, object value)
        {
            if (!(value is decimal decimalValue))
                decimalValue = Convert.ToDecimal(value);
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
