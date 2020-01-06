using System;
using System.Collections.Generic;
using System.Linq;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Types
{
    internal class EnumType : ParameterizedType
    {
        private Dictionary<string, int> values = new Dictionary<string, int>();

        public override string Name => "Enum";

        public override Type FrameworkType => typeof(string);

        public override ClickHouseTypeCode TypeCode => ClickHouseTypeCode.Enum8;

        public override ParameterizedType Parse(string typeName, Func<string, ClickHouseType> typeResolverFunc)
        {
            if (!typeName.StartsWith(Name))
                throw new ArgumentException(nameof(typeName));

            var parameters = typeName
                .Substring(Name.Length)
                .TrimRoundBrackets()
                .Split(',')
                .Select(p => p.Split('='))
                .ToDictionary(kvp => kvp[0].Trim().Trim('\''), kvp => Convert.ToInt32(kvp[1].Trim()));

            return new EnumType
            {
                values = parameters
            };
        }

        public int Lookup(string key) => values[key];

        public string Lookup(int value) => values.SingleOrDefault(kvp => kvp.Value == value).Key ?? throw new KeyNotFoundException();

        public override string ToString() => $"{Name}({string.Join(",", values.Select(kvp => kvp.Key + "=" + kvp.Value))}";
    }
}
