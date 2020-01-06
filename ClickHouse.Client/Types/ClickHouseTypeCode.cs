namespace ClickHouse.Client.Types
{
#pragma warning disable CA1720 // Identifier contains type name
    public enum ClickHouseTypeCode
    {
        Nothing,

        UInt8,
        UInt16,
        UInt32,
        UInt64,

        Int8,
        Int16,
        Int32,
        Int64,

        Float32,
        Float64,
        Decimal,

        DateTime,
        Date,

        Enum8,
        Enum16,

        String,
        FixedString,

        UUID,

        Array,
        Nested,
        Tuple,
        Nullable,
    }
#pragma warning restore CA1720 // Identifier contains type name
}