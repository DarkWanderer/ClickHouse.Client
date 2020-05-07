using ClickHouse.Client.ADO;
using ClickHouse.Client.ADO.Parameters;

namespace ClickHouse.Client.Utility
{
    public static class CommandExtensions
    {
        public static ClickHouseDbParameter AddParameter(this ClickHouseCommand command, string parameterName, object parameterValue)
        {
            var parameter = (ClickHouseDbParameter)command.CreateParameter();
            parameter.ParameterName = parameterName;
            parameter.Value = parameterValue;
            return parameter;
        }
        
        public static ClickHouseDbParameter AddParameter(this ClickHouseCommand command, string parameterName, string clickHouseType, object parameterValue)
        {
            var parameter = (ClickHouseDbParameter)command.CreateParameter();
            parameter.ParameterName = parameterName;
            parameter.ClickHouseType = clickHouseType;
            parameter.Value = parameterValue;
            return parameter;
        }
    }
}
