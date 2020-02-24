using System;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

/// <summary>
/// Sourced from https://stackoverflow.com/questions/16673714/how-to-compress-http-request-on-the-fly-and-without-loading-compressed-buffer-in
/// </summary>
namespace ClickHouse.Client.Utility
{

    public class CompressedContent : HttpContent
    {
        private readonly HttpContent originalContent;
        private readonly string encodingType;

        public CompressedContent(HttpContent content, string encodingType)
        {
            originalContent = content ?? throw new ArgumentNullException("content");
            this.encodingType = encodingType?.ToLowerInvariant() ?? throw new ArgumentNullException("encodingType");

            if (this.encodingType != "gzip" && this.encodingType != "deflate")
            {
                throw new InvalidOperationException(string.Format("Encoding '{0}' is not supported. Only supports gzip or deflate encoding.", this.encodingType));
            }

            foreach (var header in originalContent.Headers)
            {
                Headers.TryAddWithoutValidation(header.Key, header.Value);
            }

            Headers.ContentEncoding.Add(encodingType);
        }

        protected override bool TryComputeLength(out long length)
        {
            length = -1;
            return false;
        }

        protected override Task SerializeToStreamAsync(Stream stream, TransportContext context)
        {
            Stream compressedStream = encodingType switch
            {
                "gzip" => new GZipStream(stream, CompressionLevel.Fastest, leaveOpen: true),
                "deflate" => new DeflateStream(stream, CompressionMode.Compress, leaveOpen: true),
                _ => throw new ArgumentOutOfRangeException(nameof(encodingType))
            };

            return originalContent.CopyToAsync(compressedStream).ContinueWith(tsk =>
            {
                compressedStream.Dispose();
            });
        }
    }
}
