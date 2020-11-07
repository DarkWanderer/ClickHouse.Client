using System.Text;

namespace ClickHouse.Client.Utility
{
    public static class StringExtensions
    {
        public static string Escape(this string str) => "'" + str.Replace("\\", "\\\\").Replace("\'", "\\\'") + "'";
    }
}
