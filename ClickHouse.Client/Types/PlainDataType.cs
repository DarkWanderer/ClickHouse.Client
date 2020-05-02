using System;
using System.Globalization;
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
        
        public override string ToStringParameter(object value)
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
                _ => throw new NotSupportedException($"Cannot convert value {value} to type {dataType}")
            };
        }
        
    }
}
