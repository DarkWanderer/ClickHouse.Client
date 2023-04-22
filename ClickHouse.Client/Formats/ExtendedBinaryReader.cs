using System.IO;
using System.Text;

namespace ClickHouse.Client.Formats;

internal class ExtendedBinaryReader : BinaryReader
{
    private readonly PeekableStreamWrapper streamWrapper;

    public ExtendedBinaryReader(Stream stream)
        : base(new PeekableStreamWrapper(stream), Encoding.UTF8, false)
    {
        streamWrapper = (PeekableStreamWrapper)BaseStream;
    }

    public new int Read7BitEncodedInt() => base.Read7BitEncodedInt();

    /// <summary>
    /// Performs guaranteed read of requested number of bytes, or throws an exception
    /// </summary>
    /// <param name="count">number of bytes to read</param>
    /// <returns>number of bytes read, always equals to count</returns>
    /// <exception cref="EndOfStreamException">thrown if requested number of bytes is not available</exception>
    public override byte[] ReadBytes(int count)
    {
        var buffer = new byte[count];
        Read(buffer, 0, count);
        return buffer;
    }

    /// <summary>
    /// Performs guaranteed read of requested number of bytes, or throws an exception
    /// </summary>
    /// <param name="buffer">buffer array</param>
    /// <param name="index">index to write to in the buffer</param>
    /// <param name="count">number of bytes to read</param>
    /// <returns>number of bytes read, always equals to count</returns>
    /// <exception cref="EndOfStreamException">thrown if requested number of bytes is not available</exception>
    public override int Read(byte[] buffer, int index, int count)
    {
        int bytesRead = 0;
        do
        {
            int read = base.Read(buffer, index + bytesRead, count - bytesRead);
            bytesRead += read;
            if (read == 0 && bytesRead < count)
            {
                throw new EndOfStreamException($"Expected to read {count} bytes, got {bytesRead}");
            }
        }
        while (bytesRead < count);

        return bytesRead;
    }

    public override int PeekChar() => streamWrapper.Peek();
}
