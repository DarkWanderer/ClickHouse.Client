using System;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using ClickHouse.Client.Types;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.ADO.Readers
{
    internal class ClickHouseTsvReader : ClickHouseDataReader
    {
        private readonly TextReader inputReader;

        public ClickHouseTsvReader(HttpResponseMessage httpResponse)
            : base(httpResponse)
        {
            inputReader = new StreamReader(httpResponse.Content.ReadAsStreamAsync().GetAwaiter().GetResult());
            ReadHeaders();
        }

        private bool MoreRows => inputReader.Peek() != -1;

        public override bool Read()
        {
            if (!MoreRows)
            {
                return false;
            }

            var rowItems = inputReader.ReadLine().Split('\t');
            if (rowItems.Length != FieldCount)
            {
                throw new InvalidOperationException($"Wrong number of items in row ({rowItems.Length}), expected {FieldCount}");
            }

            var rowData = new object[FieldCount];
            for (var i = 0; i < FieldCount; i++)
            {
                var typeInfo = RawTypes[i];
                rowData[i] = ConvertString(rowItems[i], typeInfo);
            }
            CurrentRow = rowData;
            return true;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                inputReader.Dispose();
            }
        }

        [SuppressMessage("Style", "IDE0066:Convert switch statement to expression", Justification = "Switch statement is easier to debug")]
        private object ConvertString(string item, ClickHouseType typeInfo)
        {
            switch (typeInfo)
            {
                case ArrayType ati:
                    return item
                      .Trim('[', ']')
                      .Split(',')
                      .Select(v => ConvertString(v, ati.UnderlyingType))
                      .ToArray();
                case TupleType tti:
                    return ParseTuple(item, tti);
                case NothingType ti:
                    return item == "\\N" ? DBNull.Value : throw new InvalidOperationException();
                case NullableType nti:
                    return item == "NULL" ? DBNull.Value : ConvertString(item, nti.UnderlyingType);
                case PlainDataType<Guid> _:
                    return new Guid(item);
                case PlainDataType<IPAddress> _:
                    return IPAddress.Parse(item);
                default:
                    return Convert.ChangeType(Regex.Unescape(item), typeInfo.FrameworkType, CultureInfo.InvariantCulture);
            }
        }

        private ITuple ParseTuple(string item, TupleType tti)
        {
            var trimmed = item.TrimBrackets('(', ')');
            var types = tti.UnderlyingTypes;
            var items = trimmed.Split(',');
            var contents = new object[types.Length];
            for (var i = 0; i < types.Length; i++)
            {
                contents[i] = ConvertString(items[i].Trim('\''), types[i]);
                if (contents[i] is DBNull)
                {
                    contents[i] = null;
                }
            }
            return tti.MakeTuple(contents);
        }

        private void ReadHeaders()
        {
            var names = inputReader.ReadLine().Split('\t');
            var types = inputReader.ReadLine().Split('\t');

            if (names.Length != types.Length)
            {
                throw new InvalidOperationException($"Count mismatch between names ({names.Length}) and types ({types.Length})");
            }

            var fieldCount = names.Length;
            RawTypes = new ClickHouseType[fieldCount];
            FieldNames = new string[fieldCount];

            names.CopyTo(FieldNames, 0);
            for (var i = 0; i < fieldCount; i++)
            {
                RawTypes[i] = TypeConverter.ParseClickHouseType(Regex.Unescape(types[i]));
            }
        }
    }
}