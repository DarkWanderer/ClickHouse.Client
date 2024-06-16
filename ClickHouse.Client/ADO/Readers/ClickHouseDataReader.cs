using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Client.Formats;
using ClickHouse.Client.Numerics;
using ClickHouse.Client.Types;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.ADO.Readers;

public class ClickHouseDataReader : DbDataReader, IEnumerator<IDataReader>, IEnumerable<IDataReader>, IDataRecord
{
    private const int BufferSize = 512 * 1024;

    private readonly HttpResponseMessage httpResponse; // Used to dispose at the end of reader
    private readonly ExtendedBinaryReader reader;

    private ClickHouseDataReader(HttpResponseMessage httpResponse, ExtendedBinaryReader reader, string[] names, ClickHouseType[] types)
    {
        this.httpResponse = httpResponse ?? throw new ArgumentNullException(nameof(httpResponse));
        this.reader = reader ?? throw new ArgumentNullException(nameof(reader));
        RawTypes = types;
        FieldNames = names;
        CurrentRow = new object[FieldNames.Length];
    }

    internal static ClickHouseDataReader FromHttpResponse(HttpResponseMessage httpResponse, TypeSettings settings)
    {
        if (httpResponse is null) throw new ArgumentNullException(nameof(httpResponse));
        ExtendedBinaryReader reader = null;
        try
        {
            var stream = new BufferedStream(httpResponse.Content.ReadAsStreamAsync().GetAwaiter().GetResult(), BufferSize);
            reader = new ExtendedBinaryReader(stream); // will dispose of stream
            var (names, types) = ReadHeaders(reader, settings);
            return new ClickHouseDataReader(httpResponse, reader, names, types);
        }
        catch (Exception)
        {
            httpResponse?.Dispose();
            reader?.Dispose();
            throw;
        }
    }

    internal ClickHouseType GetEffectiveClickHouseType(int ordinal)
    {
        var type = RawTypes[ordinal];
        return type is NullableType nt ? nt.UnderlyingType : type;
    }

    internal ClickHouseType GetClickHouseType(int ordinal) => RawTypes[ordinal];

    public override object this[int ordinal] => GetValue(ordinal);

    public override object this[string name] => this[GetOrdinal(name)];

    public override int Depth { get; }

    public override int FieldCount => RawTypes?.Length ?? throw new InvalidOperationException();

    public override bool IsClosed => false;

    public sealed override bool HasRows => true;

    public override int RecordsAffected { get; }

    protected object[] CurrentRow { get; set; }

    protected string[] FieldNames { get; set; }

    private protected ClickHouseType[] RawTypes { get; set; }

    public override bool GetBoolean(int ordinal) => Convert.ToBoolean(GetValue(ordinal), CultureInfo.InvariantCulture);

    public override byte GetByte(int ordinal) => (byte)GetValue(ordinal);

    public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length) => throw new NotImplementedException();

    public override char GetChar(int ordinal) => (char)GetValue(ordinal);

    public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length) => throw new NotImplementedException();

    public override string GetDataTypeName(int ordinal) => GetClickHouseType(ordinal).ToString();

    public override DateTime GetDateTime(int ordinal) => (DateTime)GetValue(ordinal);

    public virtual DateTimeOffset GetDateTimeOffset(int ordinal) => GetEffectiveClickHouseType(ordinal) is AbstractDateTimeType adt ?
        adt.ToDateTimeOffset(GetDateTime(ordinal)) : throw new InvalidCastException();

    public override decimal GetDecimal(int ordinal)
    {
        var value = GetValue(ordinal);
        return value is ClickHouseDecimal clickHouseDecimal ? clickHouseDecimal.ToDecimal(CultureInfo.InvariantCulture) : (decimal)value;
    }

    public override double GetDouble(int ordinal) => (double)GetValue(ordinal);

    public override Type GetFieldType(int ordinal)
    {
        var rawType = RawTypes[ordinal];
        return rawType is NullableType nt ? nt.UnderlyingType.FrameworkType : rawType.FrameworkType;
    }

    public override float GetFloat(int ordinal) => (float)GetValue(ordinal);

    public override Guid GetGuid(int ordinal) => (Guid)GetValue(ordinal);

    public override short GetInt16(int ordinal) => (short)GetValue(ordinal);

    public override int GetInt32(int ordinal) => (int)GetValue(ordinal);

    public override long GetInt64(int ordinal) => (long)GetValue(ordinal);

    public override string GetName(int ordinal) => FieldNames[ordinal];

    public override int GetOrdinal(string name)
    {
        var index = Array.FindIndex(FieldNames, (fn) => fn == name);
        if (index == -1)
        {
            throw new ArgumentException("Column does not exist", nameof(name));
        }

        return index;
    }

    public override string GetString(int ordinal) => (string)GetValue(ordinal);

    public override object GetValue(int ordinal) => CurrentRow[ordinal];

    public override int GetValues(object[] values)
    {
        if (CurrentRow == null)
        {
            throw new InvalidOperationException();
        }

        CurrentRow.CopyTo(values, 0);
        return CurrentRow.Length;
    }

    public override bool IsDBNull(int ordinal)
    {
        var value = GetValue(ordinal);
        return value is DBNull || value is null;
    }

    public override bool NextResult() => false;

    public override void Close() => Dispose();

    public override T GetFieldValue<T>(int ordinal) => (T)GetValue(ordinal);

    public override DataTable GetSchemaTable() => SchemaDescriber.DescribeSchema(this);

    public override Task<bool> NextResultAsync(CancellationToken cancellationToken) => Task.FromResult(false);

    // Custom extension
    public ushort GetUInt16(int ordinal) => (ushort)GetValue(ordinal);

    // Custom extension
    public uint GetUInt32(int ordinal) => (uint)GetValue(ordinal);

    // Custom extension
    public ulong GetUInt64(int ordinal) => (ulong)GetValue(ordinal);

    // Custom extension
    public IPAddress GetIPAddress(int ordinal) => (IPAddress)GetValue(ordinal);

#if !NET462
    // Custom extension
    public ITuple GetTuple(int ordinal) => (ITuple)GetValue(ordinal);
#endif

    // Custom extension
    public sbyte GetSByte(int ordinal) => (sbyte)GetValue(ordinal);

    // Custom extension
    public BigInteger GetBigInteger(int ordinal) => (BigInteger)GetValue(ordinal);

    public override bool Read()
    {
        if (reader.PeekChar() == -1)
            return false; // End of stream reached

        var count = RawTypes.Length;
        var data = CurrentRow;
        for (var i = 0; i < count; i++)
        {
            var rawType = RawTypes[i];
            data[i] = rawType.Read(reader);
        }
        return true;
    }

#pragma warning disable CA2215 // Dispose methods should call base class dispose
    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            httpResponse?.Dispose();
            reader?.Dispose();
        }
    }
#pragma warning restore CA2215 // Dispose methods should call base class dispose

    private static (string[], ClickHouseType[]) ReadHeaders(ExtendedBinaryReader reader, TypeSettings settings)
    {
        if (reader.PeekChar() == -1)
        {
            return ([], []);
        }
        var count = reader.Read7BitEncodedInt();
        var names = new string[count];
        var types = new ClickHouseType[count];

        for (var i = 0; i < count; i++)
        {
            names[i] = reader.ReadString();
        }

        for (var i = 0; i < count; i++)
        {
            var chType = reader.ReadString();
            types[i] = TypeConverter.ParseClickHouseType(chType, settings);
        }
        return (names, types);
    }

    public bool MoveNext() => Read();

    public void Reset() => throw new NotSupportedException();

    public override IEnumerator GetEnumerator() => this;

    IEnumerator<IDataReader> IEnumerable<IDataReader>.GetEnumerator() => this;

    public IDataReader Current => this;

    object IEnumerator.Current => this;
}
