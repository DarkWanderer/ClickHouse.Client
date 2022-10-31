using System;
using System.Net;
using ClickHouse.Client.Formats;

namespace ClickHouse.Client.Types;

internal class IPv6Type : ClickHouseType
{
    public override Type FrameworkType => typeof(IPAddress);

    public override object Read(ExtendedBinaryReader reader) => new IPAddress(reader.ReadBytes(16));

    public override string ToString() => "IPv6";

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        var address6 = value is IPAddress a ? a : IPAddress.Parse((string)value);
        if (address6.AddressFamily != System.Net.Sockets.AddressFamily.InterNetworkV6)
        {
            throw new ArgumentException($"Expected IPv6, got {address6.AddressFamily}");
        }

        var ipv6bytes = address6.GetAddressBytes();
        writer.Write(ipv6bytes, 0, ipv6bytes.Length);
    }
}
