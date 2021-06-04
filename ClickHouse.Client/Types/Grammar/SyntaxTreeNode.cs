using System;
using System.Collections.Generic;
using System.Text;

namespace ClickHouse.Client.Types.Grammar
{
    public class SyntaxTreeNode
    {
        public string Value { get; set; }

        public string Name
        {
            get
            {
                if (NestedChild && Level > 0)
                {
                    int index = Value.LastIndexOf(' ');
                    if (index > 0)
                    {
                        return Value.Substring(0, index);
                    }
                }
                return null;
            }
        }

        public string ParsedValue
        {
            get
            {
                if (NestedChild && Level > 0)
                {
                    // TODO: clean "Value" from back-ticks/weird values?!
                    int index = Value.LastIndexOf(' ');
                    if (index > 0)
                    {
                        return Value.Substring(index+1);
                    }
                }
                return Value;
            }
        }

        public uint Level { get; set; }

        public bool NestedChild { get; set; }

        public bool Virtual { get; set; }

        public IList<SyntaxTreeNode> ChildNodes { get; } = new List<SyntaxTreeNode>();

        public SyntaxTreeNode SingleChild => ChildNodes.Count == 1 ? ChildNodes[0] : throw new ArgumentOutOfRangeException();

        public override string ToString()
        {
            var builder = new StringBuilder();
            builder.Append(Value);
            if (ChildNodes.Count > 0)
            {
                builder.Append("(");
                builder.Append(string.Join(", ", ChildNodes));
                builder.Append(")");
            }
            return builder.ToString();
        }
    }
}
