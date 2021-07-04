using System;
using System.Collections;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using ClickHouse.Client.ADO.Parameters;
using ClickHouse.Client.Types;

namespace ClickHouse.Client.Formats
{
    public static class HttpParameterFormatter
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
            switch (type)
            {
                case NothingType nt:
                    return "null";
                case IntegerType it:
                    return Convert.ToString(value, CultureInfo.InvariantCulture);
                case FloatType ft:
                    return FormatFloat(value);
                case DecimalType dt:
                    return Convert.ToDecimal(value).ToString(CultureInfo.InvariantCulture);
                case DateType dt:
                    return ExtractUtc(value).ToString("yyyy-MM-dd");

                case StringType st:
                case FixedStringType tt:
                case Enum8Type e8t:
                case Enum16Type e16t:
                case IPv4Type ip4:
                case IPv6Type ip6:
                    return value.ToString();

                case LowCardinalityType lt:
                    return Format(lt.UnderlyingType, value);

                case DateTimeType dtt when value is DateTime dt:
                    return dtt.TimeZone == null
                        ? $"{dt:yyyy-MM-dd HH:mm:ss}"
                        : $"{dt.ToUniversalTime():yyyy-MM-dd HH:mm:ss}";

                case DateTimeType dtt when value is DateTimeOffset dto:
                    return dtt.TimeZone == null
                        ? $"{dto:yyyy-MM-dd HH:mm:ss}"
                        : $"{dto.ToUniversalTime():yyyy-MM-dd HH:mm:ss}";

                case DateTime64Type dtt when value is DateTimeOffset dto:
                    return dtt.TimeZone == null
                        ? $"{dto:yyyy-MM-dd HH:mm:ss.fffffff}"
                        : $"{dto.ToUniversalTime():yyyy-MM-dd HH:mm:ss.fffffff}";

                case NullableType nt:
                    return value is null || value is DBNull ? "null" : $"{Format(nt.UnderlyingType, value)}";

                case ArrayType at when value is IEnumerable enumerable:
                    return $"[{string.Join(",", enumerable.Cast<object>().Select(obj => InlineParameterFormatter.Format(at.UnderlyingType, obj)))}]";

                case TupleType tupleType when value is ITuple tuple:
                    return $"({string.Join(",", tupleType.UnderlyingTypes.Select((x, i) => InlineParameterFormatter.Format(x, tuple[i])))})";

                default:
                    throw new Exception($"Cannot convert {value} to {type}");
            }
        }

        private static DateTime ExtractUtc(object value) => value switch
        {
            DateTime dt => dt.ToUniversalTime(),
            DateTimeOffset dto => dto.ToUniversalTime().DateTime,
            _ => throw new NotSupportedException($"Cannot convert value {value} to date/time type")
        };

        private static string FormatFloat(object value) => value switch
        {
            float floatValue => floatValue.ToString(CultureInfo.InvariantCulture),
            double doubleValue => doubleValue.ToString(CultureInfo.InvariantCulture),
            _ => throw new NotSupportedException($"Cannot convert value {value} to float type")
        };
    }
}
