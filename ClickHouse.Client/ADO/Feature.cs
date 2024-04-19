using System;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.ADO;

[Flags]
public enum Feature
{
    None = 0, // Special value

    [SinceVersion("21.4")]
    UUIDParameters = 32,

    [SinceVersion("21.4")]
    Map = 64,

    [SinceVersion("21.12")]
    Bool = 128,

    [SinceVersion("21.9")]
    Date32 = 256,

    [SinceVersion("21.9")]
    WideTypes = 512,

    [Obsolete]
    [SinceVersion("20.5")]
    Geo = 1024,

    [SinceVersion("22.6")]
    Stats = 2048,

    [SinceVersion("22.8")]
    AsyncInsert = 8192,

    [SinceVersion("24.1")]
    Variant = 16384,

    All = ~None, // Special value
}
