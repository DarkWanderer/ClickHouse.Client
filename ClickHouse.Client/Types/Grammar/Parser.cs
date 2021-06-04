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
                // TODO: clean token from back-ticks?!
                bool nestedChild = false;
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
                        if ((token == "Nested" || token.EndsWith(" Nested")) && level >= 1)
                        {
                            var arryLevel = new SyntaxTreeNode { Value = "Array", Level = level, Virtual = true };
                            if (current.Value == "Array")
                            {
                                current.ChildNodes.Add(arryLevel);
                            }
                            else
                            {
                                // we're a sub nested type
                                nestedChild = true;
                                stack.Peek().ChildNodes.Add(arryLevel);
                            }
                            stack.Push(arryLevel);
                            var nested = new SyntaxTreeNode { Value = token, Level = level++, NestedChild = nestedChild };
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
