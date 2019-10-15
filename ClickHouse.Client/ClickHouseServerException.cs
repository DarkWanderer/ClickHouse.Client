using System;
using System.Runtime.Serialization;

namespace ClickHouse.Client
{
    [Serializable]
    internal class ClickHouseServerException : Exception
    {
        public ClickHouseServerException()
        {
        }

        public ClickHouseServerException(string message) : base(message)
        {
        }

        public ClickHouseServerException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected ClickHouseServerException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}