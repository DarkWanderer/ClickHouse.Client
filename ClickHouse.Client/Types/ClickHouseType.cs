﻿using System;

namespace ClickHouse.Client.Types
{
    internal abstract class ClickHouseType : ISerializationTypeVisitorAcceptor
    {
        public abstract Type FrameworkType { get; }

        public abstract object AcceptRead(ISerializationTypeVisitorReader reader);

        public abstract void AcceptWrite(ISerializationTypeVisitorWriter writer, object value);

        public override abstract string ToString();
    }
}
