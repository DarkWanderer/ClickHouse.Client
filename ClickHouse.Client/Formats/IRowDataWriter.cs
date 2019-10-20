namespace ClickHouse.Client.Formats
{
    internal interface IRowDataWriter
    {
        void WriteRow(params object[] row);
    }
}
