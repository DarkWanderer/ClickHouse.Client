using System;
using System.Data;
using System.Data.Common;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Client.ADO.Parameters;
using ClickHouse.Client.ADO.Readers;
using ClickHouse.Client.Formats;

#if NET7_0_OR_GREATER

namespace ClickHouse.Client.ADO;

public class ClickHouseCancellableCommand : ClickHouseCommand
{
    public ClickHouseCancellableConnection ClickHouseConnection => (ClickHouseCancellableConnection)DbConnection;

    internal ClickHouseParameterCollection ClickHouseParameters => (ClickHouseParameterCollection)DbParameterCollection;

    public ClickHouseCancellableCommand()
        : base()
    {
    }

    public ClickHouseCancellableCommand(ClickHouseConnection connection)
        : base(connection)
    {
    }

    private async Task CancelQuery(string queryId)
    {
        if (string.IsNullOrEmpty(queryId)) return;

        System.Diagnostics.Trace.WriteLine($"QueryId '{queryId}' is canceld.");

        using ClickHouseCommand command = ClickHouseConnection.CreateCommand();
        command.CommandText = $"KILL QUERY WHERE query_id = '{queryId}'";
        int response = await command.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

#pragma warning disable CA2215 // Dispose methods should call base class dispose
    protected override void Dispose(bool disposing)
#pragma warning restore CA2215 // Dispose methods should call base class dispose
    {
        if (disposing)
        {
            // Dispose token source with delay
            _ = Task.Run(async () =>
            {
                await Task.Delay(1000).ConfigureAwait(false);

                base.Dispose(disposing);
            });
        }
    }

    public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken) => ExecuteNonQueryAsync(new ClickHouseCancellableCommandRunner(), cancellationToken);

    public virtual async Task<int> ExecuteNonQueryAsync(ClickHouseCancellableCommandRunner runner, CancellationToken cancellationToken)
    {
        if (ClickHouseConnection == null)
            throw new InvalidOperationException("Connection is not set");

        try
        {
            using var lcts = CancellationTokenSource.CreateLinkedTokenSource(CancellationToken, cancellationToken);
            using var response = await runner.PostSqlQueryAsync(this, CommandText, lcts.Token).ConfigureAwait(false);
            using var reader = new ExtendedBinaryReader(await response.Content.ReadAsStreamAsync(lcts.Token).ConfigureAwait(false));

            return reader.PeekChar() != -1 ? reader.Read7BitEncodedInt() : 0;
        }
        catch (OperationCanceledException ex)
        {
            try
            {
                await CancelQuery(runner.QueryId).ConfigureAwait(false);
            }
            catch
            {
            }

            ExceptionDispatchInfo.Capture(ex).Throw();
        }
        catch (Exception ex)
        {
            ExceptionDispatchInfo.Capture(ex).Throw();
        }
        return -1; // no here
    }

    /// <summary>
    ///  Allows to return raw result from a query (with custom FORMAT)
    /// </summary>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>ClickHouseRawResult object containing response stream</returns>
    public override Task<ClickHouseRawResult> ExecuteRawResultAsync(CancellationToken cancellationToken) => ExecuteRawResultAsync(new ClickHouseCancellableCommandRunner(), cancellationToken);

    public virtual async Task<ClickHouseRawResult> ExecuteRawResultAsync(ClickHouseCancellableCommandRunner runner, CancellationToken cancellationToken)
    {
        if (ClickHouseConnection == null)
            throw new InvalidOperationException("Connection is not set");

        try
        {
            using var lcts = CancellationTokenSource.CreateLinkedTokenSource(CancellationToken, cancellationToken);
            var response = await runner.PostSqlQueryAsync(this, CommandText, lcts.Token).ConfigureAwait(false);
            return new ClickHouseRawResult(response);
        }
        catch (OperationCanceledException ex)
        {
            try
            {
                await CancelQuery(runner.QueryId).ConfigureAwait(false);
            }
            catch
            {
            }

            ExceptionDispatchInfo.Capture(ex).Throw();
        }
        catch (Exception ex)
        {
            ExceptionDispatchInfo.Capture(ex).Throw();
        }
        return null; // no here
    }

    public override Task<object> ExecuteScalarAsync(CancellationToken cancellationToken) => ExecuteScalarAsync(new ClickHouseCancellableCommandRunner(), cancellationToken);

    public virtual async Task<object> ExecuteScalarAsync(ClickHouseCancellableCommandRunner runner, CancellationToken cancellationToken)
    {
        using var lcts = CancellationTokenSource.CreateLinkedTokenSource(CancellationToken, cancellationToken);
        using var reader = await ExecuteDbDataReaderAsync(runner, CommandBehavior.Default, lcts.Token).ConfigureAwait(false);
        return reader.Read() ? reader.GetValue(0) : null;
    }

    protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken) =>
        ExecuteDbDataReaderAsync(new ClickHouseCancellableCommandRunner(), behavior, cancellationToken);

    protected virtual async Task<DbDataReader> ExecuteDbDataReaderAsync(ClickHouseCancellableCommandRunner runner, CommandBehavior behavior, CancellationToken cancellationToken)
    {
        if (ClickHouseConnection == null)
            throw new InvalidOperationException("Connection is not set");

        try
        {
            using var lcts = CancellationTokenSource.CreateLinkedTokenSource(CancellationToken, cancellationToken);
            var sqlBuilder = new StringBuilder(CommandText);
            switch (behavior)
            {
                case CommandBehavior.SingleRow:
                    sqlBuilder.Append(" LIMIT 1");
                    break;
                case CommandBehavior.SchemaOnly:
                    sqlBuilder.Append(" LIMIT 0");
                    break;
                default:
                    break;
            }
            var result = await runner.PostSqlQueryAsync(this, sqlBuilder.ToString(), lcts.Token).ConfigureAwait(false);
            return ClickHouseDataReader.FromHttpResponse(result, ClickHouseConnection.TypeSettings);
        }
        catch (OperationCanceledException ex)
        {
            try
            {
                await CancelQuery(runner.QueryId).ConfigureAwait(false);
            }
            catch
            {
            }

            ExceptionDispatchInfo.Capture(ex).Throw();
        }
        catch (Exception ex)
        {
            ExceptionDispatchInfo.Capture(ex).Throw();
        }
        return null; // no here
    }
}
#endif
