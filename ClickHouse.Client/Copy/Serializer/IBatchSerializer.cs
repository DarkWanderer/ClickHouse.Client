using System.IO;

namespace ClickHouse.Client.Copy.Serializer;

internal interface IBatchSerializer
{
    void Serialize(Batch batch, Stream stream);
}
