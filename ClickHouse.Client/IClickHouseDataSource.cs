#if NET7_0_OR_GREATER
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Client;

public interface IClickHouseDataSource
{
    string ConnectionString {
        get;
    }

    IClickHouseConnection CreateConnection();

    IClickHouseConnection OpenConnection();

    Task<IClickHouseConnection> OpenConnectionAsync(CancellationToken cancellationToken = default);
}
#endif
