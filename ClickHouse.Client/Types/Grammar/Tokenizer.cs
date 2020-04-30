using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ClickHouse.Client.Types.Grammar
{
    public static class Tokenizer
    {
        private static char[] breaks = new[] { ',', '(', ')' };

        public static IEnumerable<string> GetTokens(string input)
        {
            var start = 0;
            var len = input.Length;

            while (start < len)
            {
                var nextBreak = input.IndexOfAny(breaks, start);
                if (nextBreak == start)
                {
                    start++;
                    yield return input.Substring(nextBreak, 1);
                }
                else if (nextBreak == -1)
                {
                    yield return input.Substring(start).Trim();
                    yield break;
                }
                else
                {
                    yield return input.Substring(start, nextBreak - start).Trim();
                    start = nextBreak;
                }
            }
        }
    }

    public static class Parser
    {
        public static SyntaxTreeNode Parse(string input)
        {
            var tokens = Tokenizer.GetTokens(input).ToList();
            var stack = new Stack<SyntaxTreeNode>();
            SyntaxTreeNode current = null;

            foreach (var token in tokens)
            {
                switch (token)
                {
                    case "(":
                        stack.Push(current);
                        break;
                    case ",":
                        stack.Peek().ChildNodes.Add(current);
                        break;
                    case ")":
                        stack.Peek().ChildNodes.Add(current);
                        current = stack.Pop();
                        break;
                    default:
                        current = new SyntaxTreeNode { Value = token };
                        break;
                }
            }
            return current;
        }
    }
}
