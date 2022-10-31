using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NodaTime;

namespace ClickHouse.Client;

internal record struct TypeSettings(bool useBigDecimal, string timezone)
{
    public static string DefaultTimezone = DateTimeZoneProviders.Tzdb.GetSystemDefault().Id;

    public static TypeSettings Default => new TypeSettings(false, DefaultTimezone);
}
