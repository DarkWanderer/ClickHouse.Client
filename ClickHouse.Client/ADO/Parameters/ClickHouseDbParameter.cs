﻿using System.Data;
using System.Data.Common;
using ClickHouse.Client.Types;

namespace ClickHouse.Client.ADO.Parameters
{
    public class ClickHouseDbParameter : DbParameter
    {
        public override DbType DbType { get; set; }
        
        public string ClickHouseType { get; set; }

        public override ParameterDirection Direction { get; set; }

        public override bool IsNullable { get; set; }

        public override string ParameterName { get; set; }

        public override int Size { get; set; }

        public override string SourceColumn { get; set; }

        public override bool SourceColumnNullMapping { get; set; }

        public override object Value { get; set; }

        public override void ResetDbType() { }
    }
}
