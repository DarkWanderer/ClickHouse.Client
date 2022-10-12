using System;
using System.Collections;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using ClickHouse.Client.ADO.Parameters;
using ClickHouse.Client.Numerics;
using ClickHouse.Client.Types;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Formats
{
    internal static class HttpParameterFormatter
    {
        private const string NullValueString = "\\N";

        public static string Format(ClickHouseDbParameter parameter, TypeSettings settings)
        {
            var type = string.IsNullOrWhiteSpace(parameter.ClickHouseType)
                ? TypeConverter.ToClickHouseType(parameter.Value.GetType())
                : TypeConverter.ParseClickHouseType(parameter.ClickHouseType, settings);
            return Format(type, parameter.Value, false);
        }

        internal static string Format(ClickHouseType type, object value, bool quote)
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
                case DecimalType dt when value is ClickHouseDecimal chd:
                    return chd.ToString(CultureInfo.InvariantCulture);
                case DecimalType dt:
                    return Convert.ToDecimal(value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture);

                case DateType dt when value is DateTimeOffset @do:
                    return @do.Date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                case DateType dt:
                    return Convert.ToDateTime(value, CultureInfo.InvariantCulture).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                case Date32Type dt when value is DateTimeOffset @do:
                    return @do.Date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                case Date32Type dt:
                    return Convert.ToDateTime(value, CultureInfo.InvariantCulture).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

                case StringType st:
                case FixedStringType tt:
                case Enum8Type e8t:
                case Enum16Type e16t:
                case IPv4Type ip4:
                case IPv6Type ip6:
                case UuidType uuidType:
                    return quote ? value.ToString().Escape().QuoteSingle() : value.ToString().Escape();

                case LowCardinalityType lt:
                    return Format(lt.UnderlyingType, value, quote);

                case DateTimeType dtt when value is DateTime dt:
                    return dt.ToString("s", CultureInfo.InvariantCulture);

                case DateTimeType dtt when value is DateTimeOffset dto:
                    return dto.ToString("s", CultureInfo.InvariantCulture);

                case DateTime64Type dtt when value is DateTime dtv:
                    return $"{dtv:yyyy-MM-dd HH:mm:ss.fffffff}";

                case DateTime64Type dtt when value is DateTimeOffset dto:
                    return $"{dto:yyyy-MM-dd HH:mm:ss.fffffff}";

                case NullableType nt:
                    return value is null || value is DBNull ? quote ? "null" : NullValueString : Format(nt.UnderlyingType, value, quote);

                case ArrayType arrayType when value is IEnumerable enumerable:
                    return $"[{string.Join(",", enumerable.Cast<object>().Select(obj => Format(arrayType.UnderlyingType, obj, true)))}]";

                case TupleType tupleType when value is ITuple tuple:
                    return $"({string.Join(",", tupleType.UnderlyingTypes.Select((x, i) => Format(x, tuple[i], true)))})";

                case MapType mapType when value is IDictionary dict:
                    var strings = string.Join(",", dict.Keys.Cast<object>().Select(k => $"{Format(mapType.KeyType, k, true)} : {Format(mapType.ValueType, dict[k], true)}"));
                    return $"{{{string.Join(",", strings)}}}";

                default:
                    throw new ArgumentException($"Cannot convert {value} to {type}");
            }
        }
    }
}
