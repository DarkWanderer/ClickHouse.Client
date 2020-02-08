using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClickHouse.Client.Utility
{
    public static class MathUtils
    {
        public static ulong IntPow(uint value, int power)
        {
            ulong result = 1;
            while (power > 0)
            {
                if ((power & 1) == 1)
                    result *= value;
                power = power >> 1;
                if (power <= 0)
                    break;
                value *= value;
            }
            return result;
        }
    }
}
