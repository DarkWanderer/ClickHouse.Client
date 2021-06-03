using System.Collections.Generic;
using System.Linq;

namespace ClickHouse.Client.Types.Grammar
{
    public static class Parser
    {
        public static SyntaxTreeNode Parse(string input)
        {
            var tokens = Tokenizer.GetTokens(input).ToList();
            
            var stack = new Stack<SyntaxTreeNode>();
            SyntaxTreeNode current = null;
            uint level = 0;
            foreach (var token in tokens)
            {
                switch (token)
                {
                    case "(":
                        stack.Push(current);
                        level++;
                        break;
                    case ",":
                        stack.Peek().ChildNodes.Add(current);
                        break;
                    case ")":
                        var peek = stack.Peek();
                        if (!peek.Virtual)
                        {
                            peek.ChildNodes.Add(current);
                        }
                        current = stack.Pop();
                        level--;
                        if (current.Virtual)
                        {
                            current = stack.Pop();
                            level--;
                        }
                        break;
                    default:
                        if (token == "Nested" && level == 1)
                        {
                            var arryLevel = new SyntaxTreeNode { Value = "Array", Level = level, Virtual = true };
                            current.ChildNodes.Add(arryLevel);
                            stack.Push(arryLevel);
                            var nested = new SyntaxTreeNode { Value = token, Level = level++ };
                            arryLevel.ChildNodes.Add(nested);
                            current = nested;
                        }
                        else
                        {
                            current = new SyntaxTreeNode { Value = token, Level = level };
                        }
                        break;
                }
            }
            return current;
        }
    }
}
