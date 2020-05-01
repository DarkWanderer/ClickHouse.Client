using System.Data;
using System.Data.Common;
using System.Threading.Tasks;
using ClickHouse.Client.ADO.Adapters;

namespace ClickHouse.Client.Utility
{
    public static class CommandExtensions
    {
        public static DbParameter AddParameter(this DbCommand command, string parameterName, object parameterValue)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = parameterName;
            parameter.Value = parameterValue;
            return parameter;
        }
    }
}
