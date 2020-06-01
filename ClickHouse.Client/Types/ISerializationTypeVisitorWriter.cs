namespace ClickHouse.Client.Types
{
    internal interface ISerializationTypeVisitorWriter
    {
        void VisitWrite(LowCardinalityType lowCardinalityType, object value);

        void VisitWrite(FixedStringType fixedStringType, object value);

        void Write(Int8Type int8Type, object value);

        void Write(UInt32Type uInt32Type, object value);

        void Write(Int32Type int32Type, object value);

        void Write(UInt16Type uInt16Type, object value);

        void Write(Int16Type int16Type, object value);

        void Write(UInt8Type uInt8Type, object value);
        void Write(Int64Type int64Type, object value);
        void Write(UInt64Type uInt64Type, object value);
    }
}
