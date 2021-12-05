using System;
using System.Globalization;
using System.Text;
using ClickHouse.Client.Formats;
using ClickHouse.Client.Types.Grammar;

namespace ClickHouse.Client.Types
{
    internal class FixedStringType : ParameterizedType
    {
        public int Length { get; set; }

        public override Type FrameworkType => typeof(string);

        public override string Name => "FixedString";

        public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc)
        {
            return new FixedStringType
            {
                Length = int.Parse(node.SingleChild.Value),
            };
        }

        public override string ToString() => $"FixedString{Length}";

        public override object Read(ExtendedBinaryReader reader) => Encoding.UTF8.GetString(reader.ReadBytes(Length));

        public override void Write(ExtendedBinaryWriter writer, object value)
        {
            var @string = Convert.ToString(value, CultureInfo.InvariantCulture);
            var stringBytes = new byte[Length];
            Encoding.UTF8.GetBytes(@string, 0, @string.Length, stringBytes, 0);
            writer.Write(stringBytes);
        }
    }
}
