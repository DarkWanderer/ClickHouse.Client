using System.Data;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.ADO.Parameters;

namespace ClickHouse.Client
{
    public interface IClickHouseCommand : IDbCommand
    {
        /// <summary>
        /// Creates an instance of <see cref="ClickHouseDbParameter"/>.
        /// </summary>
        /// <returns>A <see cref="ClickHouseDbParameter"> object.</returns>
        new ClickHouseDbParameter CreateParameter();

        /// <summary>
        /// Sets a query parameter that will be added to the query request.
        /// </summary>
        /// <param name="name">The name of the query parameter.</param>
        /// <param name="value">The value of the query parameter.</param>
        /// <remarks>
        /// <list type="bullet">
        /// <item>All query parameters are scoped to each <seealso cref="IClickHouseCommand"/> only.</item>
        /// <item>Adding new value for the same parameter name overrides the old value.</item>
        /// </list>
        /// </remarks>
        public void SetQueryParameter(string name, string value);

        /// <summary>
        /// Removes the specified query parameter. The removed parameters do not get included in the query request.
        /// </summary>
        /// <param name="name">The name of the query parameter that should be removed.</param>
        public void RemoveQueryParameter(string name);

        /// <summary>
        ///  Allows to return raw result from a query (with custom FORMAT)
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>ClickHouseRawResult object containing response stream</returns>
        Task<ClickHouseRawResult> ExecuteRawResultAsync(CancellationToken cancellationToken);
    }
}
