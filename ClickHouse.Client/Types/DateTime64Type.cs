using System;
using ClickHouse.Client.Utility;
using NodaTime;

namespace ClickHouse.Client.Types
{
    internal class DateTime64Type : ParameterizedType
    {
        public override ClickHouseTypeCode TypeCode => ClickHouseTypeCode.DateTime64;

        public override Type FrameworkType => typeof(DateTime);

        public DateTimeZone TimeZone { get; set; }

        public override string Name => "DateTime64";

        public int Scale { get; set; }

        public override string ToString() => $"DateTime({TimeZone.Id})";

        public override ParameterizedType Parse(string typeName, Func<string, ClickHouseType> typeResolverFunc)
        {
            if (!typeName.StartsWith(Name))
                throw new ArgumentException(nameof(typeName));

            var scale = Int32.Parse(typeName.Substring(Name.Length).TrimRoundBrackets());

            return new DateTime64Type
            {
                TimeZone = DateTimeZone.Utc,
                Scale = scale
            };
        }
    }
}
