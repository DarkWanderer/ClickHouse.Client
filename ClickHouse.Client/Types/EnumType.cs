using System;
using System.Collections.Generic;
using System.Linq;
using ClickHouse.Client.Types.Grammar;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Types
{
    internal class EnumType : ParameterizedType
    {
        private Dictionary<string, int> values = new Dictionary<string, int>();

        public override string Name => "Enum";

        public override Type FrameworkType => typeof(string);

        public override ClickHouseTypeCode TypeCode => ClickHouseTypeCode.Enum8;

        public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> typeResolverFunc)
        {
            var parameters = node.ChildNodes
                .Select(cn => cn.Value)
                .Select(p => p.Split('='))
                .ToDictionary(kvp => kvp[0].Trim().Trim('\''), kvp => Convert.ToInt32(kvp[1].Trim()));

            return new EnumType
            {
                values = parameters,
            };
        }

        public int Lookup(string key) => values[key];

        public string Lookup(int value) => values.SingleOrDefault(kvp => kvp.Value == value).Key ?? throw new KeyNotFoundException();

        public override string ToStringParameter(object value) => $"'{value.ToString().Escape()}'";

        public override string ToString() => $"{Name}({string.Join(",", values.Select(kvp => kvp.Key + "=" + kvp.Value))}";
    }
}
