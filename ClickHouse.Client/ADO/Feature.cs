using System;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.ADO;

[Flags]
public enum Feature
{
    None = 0, // Special value

    [SinceVersion("21.4")]
    UUIDParameters = 1,

    [SinceVersion("21.4")]
    Map = 1 << 1,

    [SinceVersion("21.12")]
    Bool = 1 << 2,

    [SinceVersion("21.9")]
    Date32 = 1 << 3,

    [SinceVersion("21.9")]
    WideTypes = 1 << 4,

    [Obsolete]
    [SinceVersion("20.5")]
    Geo = 1 << 5,

    [SinceVersion("22.6")]
    Stats = 1 << 6,

    [SinceVersion("22.8")]
    AsyncInsert = 1 << 7,

    [SinceVersion("24.1")]
    Variant = 1 << 8,

    [SinceVersion("22.3")]
    ParamsInMultipartFormData = 1 << 9,

    [SinceVersion("24.1")]
    Json = 1 << 10,

    [SinceVersion("25.1")]
    Dynamic = 1 << 11,

    All = ~None, // Special value
}
