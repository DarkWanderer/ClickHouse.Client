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
                ClickHouseTypeCode.UInt8 when value is byte byteValue => byteValue.ToString(),
                ClickHouseTypeCode.UInt16 when value is ushort ushortValue => ushortValue.ToString(),
                ClickHouseTypeCode.UInt32 when value is uint uintValue => uintValue.ToString(),
                ClickHouseTypeCode.UInt64 when value is ulong ulongValue => ulongValue.ToString(),
                ClickHouseTypeCode.Int8 when value is sbyte sbyteValue => sbyteValue.ToString(),
                ClickHouseTypeCode.Int16 when value is short shortValue => shortValue.ToString(),
                ClickHouseTypeCode.Int32 when value is int intValue => intValue.ToString(),
                ClickHouseTypeCode.Int64 when value is long longValue => longValue.ToString(),
                ClickHouseTypeCode.Float32 when value is float floatValue => floatValue.ToString(CultureInfo.InvariantCulture),
                ClickHouseTypeCode.Float64 when value is double doubleValue => doubleValue.ToString(CultureInfo.InvariantCulture),
                ClickHouseTypeCode.String when value is string stringValue => Uri.EscapeDataString(stringValue),
                ClickHouseTypeCode.UUID when value is Guid guidValue => guidValue.ToString(),
                ClickHouseTypeCode.IPv4 when value is IPAddress iPAddressValue => iPAddressValue.ToString(),
                ClickHouseTypeCode.IPv6 when value is IPAddress iPAddressValue => iPAddressValue.ToString(),
                ClickHouseTypeCode.Date when value is DateTime date => $"{(DateTime)value:yyyy-MM-dd}",
                _ => throw new NotSupportedException($"Cannot convert value {value} to type {dataType}")
            };
        }
        
        public override string ToInlineParameter(object value)
        {
            return dataType switch
            {
                ClickHouseTypeCode.UInt8 when value is byte byteValue => byteValue.ToString(),
                ClickHouseTypeCode.UInt16 when value is ushort ushortValue => ushortValue.ToString(),
                ClickHouseTypeCode.UInt32 when value is uint uintValue => uintValue.ToString(),
                ClickHouseTypeCode.UInt64 when value is ulong ulongValue => ulongValue.ToString(),
                ClickHouseTypeCode.Int8 when value is sbyte sbyteValue => sbyteValue.ToString(),
                ClickHouseTypeCode.Int16 when value is short shortValue => shortValue.ToString(),
                ClickHouseTypeCode.Int32 when value is int intValue => intValue.ToString(),
                ClickHouseTypeCode.Int64 when value is long longValue => longValue.ToString(),
                ClickHouseTypeCode.Float32 when value is float floatValue => floatValue.ToString(CultureInfo.InvariantCulture),
                ClickHouseTypeCode.Float64 when value is double doubleValue => doubleValue.ToString(CultureInfo.InvariantCulture),
                ClickHouseTypeCode.String when value is string stringValue => stringValue.Escape(),
                ClickHouseTypeCode.UUID when value is Guid guidValue => guidValue.ToString().Escape(),
                ClickHouseTypeCode.IPv4 when value is IPAddress iPAddressValue => iPAddressValue.ToString().Escape(),
                ClickHouseTypeCode.IPv6 when value is IPAddress iPAddressValue => iPAddressValue.ToString().Escape(),
                ClickHouseTypeCode.Date when value is DateTime date => $"'{(DateTime)value:yyyy-MM-dd}'",
                _ => throw new NotSupportedException($"Cannot convert value {value} to type {dataType}")
            };
        }
    }
}
