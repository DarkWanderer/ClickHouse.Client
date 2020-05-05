using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Types
{
    internal class PlainDataType<T> : ClickHouseType
    {
        private readonly ClickHouseTypeCode dataType;

        public PlainDataType(ClickHouseTypeCode dataType)
        {
            this.dataType = dataType;
            FrameworkType = typeof(T);
        }

        public override Type FrameworkType { get; }

        public override ClickHouseTypeCode TypeCode => dataType;

        public override string ToString() => TypeCode.ToString();
        
        public override string ToHttpParameter(object value)
        {
            if (value is IEnumerable enumerable && !(value is string))
                return $"({string.Join(',', enumerable.Cast<object>().Select(ToHttpParameter))})";

            return dataType switch
            {
                ClickHouseTypeCode.UInt8 => value.ToString(),
                ClickHouseTypeCode.UInt16 => value.ToString(),
                ClickHouseTypeCode.UInt32 => value.ToString(),
                ClickHouseTypeCode.UInt64 => value.ToString(),
                ClickHouseTypeCode.Int8 => value.ToString(),
                ClickHouseTypeCode.Int16 => value.ToString(),
                ClickHouseTypeCode.Int32 => value.ToString(),
                ClickHouseTypeCode.Int64 => value.ToString(),
                ClickHouseTypeCode.Float32 when value is float floatValue => floatValue.ToString(CultureInfo.InvariantCulture),
                ClickHouseTypeCode.Float32 when value is double doubleValue => doubleValue.ToString(CultureInfo.InvariantCulture),
                ClickHouseTypeCode.Float64 when value is float floatValue => floatValue.ToString(CultureInfo.InvariantCulture),
                ClickHouseTypeCode.Float64 when value is double doubleValue => doubleValue.ToString(CultureInfo.InvariantCulture),
                ClickHouseTypeCode.String when value is string stringValue => stringValue,
                ClickHouseTypeCode.UUID when value is Guid guidValue => guidValue.ToString(),
                ClickHouseTypeCode.IPv4 when value is IPAddress iPAddressValue => iPAddressValue.ToString(),
                ClickHouseTypeCode.IPv6 when value is IPAddress iPAddressValue => iPAddressValue.ToString(),
                ClickHouseTypeCode.Date when value is DateTime date => $"{date:yyyy-MM-dd}",
                _ => throw new NotSupportedException($"Cannot convert value {value} to type {dataType}")
            };
        }

        public override string ToHttpUnderlyingParameter(object value) => ToInlineParameter(value);
        
        public override string ToInlineParameter(object value)
        {
            return dataType switch
            {
                ClickHouseTypeCode.UInt8 => value.ToString(),
                ClickHouseTypeCode.UInt16 => value.ToString(),
                ClickHouseTypeCode.UInt32 => value.ToString(),
                ClickHouseTypeCode.UInt64 => value.ToString(),
                ClickHouseTypeCode.Int8 => value.ToString(),
                ClickHouseTypeCode.Int16 => value.ToString(),
                ClickHouseTypeCode.Int32 => value.ToString(),
                ClickHouseTypeCode.Int64 => value.ToString(),
                ClickHouseTypeCode.Float32 when value is float floatValue => floatValue.ToString(CultureInfo.InvariantCulture),
                ClickHouseTypeCode.Float32 when value is double doubleValue => doubleValue.ToString(CultureInfo.InvariantCulture),
                ClickHouseTypeCode.Float64 when value is float floatValue => floatValue.ToString(CultureInfo.InvariantCulture),
                ClickHouseTypeCode.Float64 when value is double doubleValue => doubleValue.ToString(CultureInfo.InvariantCulture),
                ClickHouseTypeCode.String when value is string stringValue => stringValue.Escape(),
                ClickHouseTypeCode.UUID when value is Guid guidValue => guidValue.ToString().Escape(),
                ClickHouseTypeCode.IPv4 when value is IPAddress iPAddressValue => $"toIPv4({iPAddressValue.ToString().Escape()})",
                ClickHouseTypeCode.IPv6 when value is IPAddress iPAddressValue => $"toIPv6({iPAddressValue.ToString().Escape()})",
                ClickHouseTypeCode.Date when value is DateTime date => $"'{date:yyyy-MM-dd}'",
                _ => throw new NotSupportedException($"Cannot convert value {value} to type {dataType}")
            };
        }
    }
}
