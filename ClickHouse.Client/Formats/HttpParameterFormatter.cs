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
        private const string NullValueString = "\\N";

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
                case NothingType nt:
                    return NullValueString;
                case BooleanType bt:
                    return (bool)value ? "true" : "false";
                case IntegerType it:
                case FloatType ft:
                    return Convert.ToString(value, CultureInfo.InvariantCulture);
                case DecimalType dt:
                    return Convert.ToDecimal(value).ToString(CultureInfo.InvariantCulture);

                case DateType dt when value is DateTimeOffset @do:
                    return @do.Date.ToString("yyyy-MM-dd");
                case DateType dt:
                    return Convert.ToDateTime(value).ToString("yyyy-MM-dd");

                case StringType st:
                case FixedStringType tt:
                case Enum8Type e8t:
                case Enum16Type e16t:
                case IPv4Type ip4:
                case IPv6Type ip6:
                case UuidType uuidType:
                    return value.ToString();

                case LowCardinalityType lt:
                    return Format(lt.UnderlyingType, value);

                case DateTimeType dtt when value is DateTime dt:
                    return dt.ToString("s", CultureInfo.InvariantCulture);

                case DateTimeType dtt when value is DateTimeOffset dto:
                    return dto.ToString("s", CultureInfo.InvariantCulture);

                case DateTime64Type dtt when value is DateTime dtv:
                    return $"{dtv:yyyy-MM-dd HH:mm:ss.fffffff}";

                case DateTime64Type dtt when value is DateTimeOffset dto:
                    return $"{dto:yyyy-MM-dd HH:mm:ss.fffffff}";

                case NullableType nt:
                    return value is null || value is DBNull ? NullValueString : $"{Format(nt.UnderlyingType, value)}";

                case ArrayType arrayType when value is IEnumerable enumerable:
                    return $"[{string.Join(",", enumerable.Cast<object>().Select(obj => InlineParameterFormatter.Format(arrayType.UnderlyingType, obj)))}]";

                case TupleType tupleType when value is ITuple tuple:
                    return $"({string.Join(",", tupleType.UnderlyingTypes.Select((x, i) => InlineParameterFormatter.Format(x, tuple[i])))})";

                case MapType mapType when value is IDictionary dict:
                    var strings = string.Join(",", dict.Keys.Cast<object>().Select(k => $"{InlineParameterFormatter.Format(mapType.KeyType, k)} : {InlineParameterFormatter.Format(mapType.ValueType, dict[k])}"));
                    return $"{{{string.Join(",", strings)}}}";

                default:
                    throw new Exception($"Cannot convert {value} to {type}");
            }
        }
    }
}
