using System.Text;

namespace ClickHouse.Client.Utility
{
    public static class StringExtensions
    {
        public static string TrimBrackets(this string input, char leftBracket, char rightBracket)
        {
            if (input[0] != leftBracket || input[input.Length - 1] != rightBracket)
                return input;

            return input.Substring(1, input.Length - 2);
        }

        public static string TrimRoundBrackets(this string input) => TrimBrackets(input, '(', ')');

        public static string TrimSquareBrackets(this string input) => TrimBrackets(input, '[', ']');

        public static string TrimCurlyBraces(this string input) => TrimBrackets(input, '{', '}');

        public static string ToHexString(this byte[] byteArray)
        {
            var hex = new StringBuilder(byteArray.Length * 2);

            foreach (var b in byteArray)
            {
                hex.AppendFormat("{0:x2} ", b);
            }
            return hex.ToString();
        }
    }
}
