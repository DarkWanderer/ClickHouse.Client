#if NET7_0_OR_GREATER
using System;
using System.Data.Common;
using System.Net;
using System.Net.Http;
using ClickHouse.Client;
using ClickHouse.Client.ADO;
using Microsoft.Extensions.DependencyInjection.Extensions;

// ReSharper disable once CheckNamespace
namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Extension method for setting up ClickHouse services in an <see cref="IServiceCollection" />.
/// </summary>
public static class ClickHouseServiceCollectionExtensions
{
    /// <summary>
    /// Registers a <see cref="ClickHouseDataSource" /> and a <see cref="ClickHouseConnection" /> in the <see cref="IServiceCollection" />.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection" /> to add services to.</param>
    /// <param name="connectionString">A ClickHouse connection string.</param>
    /// <param name="httpClient">instance of HttpClient</param>
    /// <param name="connectionLifetime">
    /// The lifetime with which to register the <see cref="ClickHouseConnection" /> in the container.
    /// Defaults to <see cref="ServiceLifetime.Transient" />.
    /// </param>
    /// <param name="dataSourceLifetime">
    /// The lifetime with which to register the <see cref="ClickHouseDataSource" /> service in the container.
    /// Defaults to <see cref="ServiceLifetime.Singleton" />.
    /// </param>
    /// <param name="serviceKey">The <see cref="ServiceDescriptor.ServiceKey"/> of the data source.</param>
    /// <returns>The same service collection so that multiple calls can be chained.</returns>
    public static IServiceCollection AddClickHouseDataSource(
        this IServiceCollection services,
        string connectionString,
        HttpClient httpClient = null,
        ServiceLifetime connectionLifetime = ServiceLifetime.Transient,
        ServiceLifetime dataSourceLifetime = ServiceLifetime.Singleton,
        object serviceKey = null) =>
        AddClickHouseDataSource(
            services,
            (_, _) =>
            {
                if (httpClient == null)
                {
                    // Ensure that we are using the same HTTP client for all connections
#pragma warning disable CA5399
                    httpClient = new HttpClient(new HttpClientHandler
                    {
                        AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
                    });
#pragma warning restore CA5399
                }

                return new ClickHouseDataSource(connectionString, httpClient);
            },
            connectionLifetime,
            dataSourceLifetime,
            serviceKey);

    /// <summary>
    /// Registers a <see cref="ClickHouseDataSource" /> and a <see cref="ClickHouseConnection" /> in the <see cref="IServiceCollection" />.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection" /> to add services to.</param>
    /// <param name="connectionString">A ClickHouse connection string.</param>
    /// <param name="httpClientFactory">The factory to be used for creating the clients.</param>
    /// <param name="httpClientName">
    /// The name of the HTTP client you want to be created using the provided factory.
    /// If left empty, the default client will be created.
    /// </param>
    /// <param name="connectionLifetime">
    /// The lifetime with which to register the <see cref="ClickHouseConnection" /> in the container.
    /// Defaults to <see cref="ServiceLifetime.Transient" />.
    /// </param>
    /// <param name="dataSourceLifetime">
    /// The lifetime with which to register the <see cref="ClickHouseDataSource" /> service in the container.
    /// Defaults to <see cref="ServiceLifetime.Singleton" />.
    /// </param>
    /// <param name="serviceKey">The <see cref="ServiceDescriptor.ServiceKey"/> of the data source.</param>
    /// <returns>The same service collection so that multiple calls can be chained.</returns>
    public static IServiceCollection AddClickHouseDataSource(
        this IServiceCollection services,
        string connectionString,
        IHttpClientFactory httpClientFactory,
        string httpClientName = "",
        ServiceLifetime connectionLifetime = ServiceLifetime.Transient,
        ServiceLifetime dataSourceLifetime = ServiceLifetime.Singleton,
        object serviceKey = null) =>
        AddClickHouseDataSource(services, (_, _) => new ClickHouseDataSource(connectionString, httpClientFactory, httpClientName), connectionLifetime, dataSourceLifetime, serviceKey);

    /// <summary>
    /// Registers a <see cref="ClickHouseDataSource" /> and a <see cref="ClickHouseConnection" /> in the <see cref="IServiceCollection" />.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection" /> to add services to.</param>
    /// <param name="dataSourceFactory">A factory for <see cref="ClickHouseDataSource" /> instances.</param>
    /// <param name="connectionLifetime">
    /// The lifetime with which to register the <see cref="ClickHouseConnection" /> in the container.
    /// Defaults to <see cref="ServiceLifetime.Transient" />.
    /// </param>
    /// <param name="dataSourceLifetime">
    /// The lifetime with which to register the <see cref="ClickHouseDataSource" /> service in the container.
    /// Defaults to <see cref="ServiceLifetime.Singleton" />.
    /// </param>
    /// <param name="serviceKey">The <see cref="ServiceDescriptor.ServiceKey"/> of the data source.</param>
    /// <returns>The same service collection so that multiple calls can be chained.</returns>
    private static IServiceCollection AddClickHouseDataSource(
        this IServiceCollection services,
        Func<IServiceProvider, object, ClickHouseDataSource> dataSourceFactory,
        ServiceLifetime connectionLifetime = ServiceLifetime.Transient,
        ServiceLifetime dataSourceLifetime = ServiceLifetime.Singleton,
        object serviceKey = null)
    {
        services.TryAdd(new ServiceDescriptor(typeof(ClickHouseDataSource), serviceKey, dataSourceFactory, dataSourceLifetime));
        services.TryAdd(new ServiceDescriptor(typeof(ClickHouseConnection), serviceKey, static (sp, key) => GetService<ClickHouseDataSource>(sp, key).CreateConnection(), connectionLifetime));

        // Try to forward common types
        AddForwardDataSource<IClickHouseDataSource>(services, dataSourceLifetime, serviceKey);
        AddForwardDataSource<DbDataSource>(services, dataSourceLifetime, serviceKey);
        AddForwardConnection<IClickHouseConnection>(services, dataSourceLifetime, serviceKey);
        AddForwardConnection<DbConnection>(services, dataSourceLifetime, serviceKey);
        return services;

        static void AddForwardConnection<T>(IServiceCollection services, ServiceLifetime lifetime, object serviceKey)
        {
            services.TryAdd(
                new ServiceDescriptor(
                    typeof(T),
                    serviceKey,
                    static (sp, key) => GetService<ClickHouseConnection>(sp, key),
                    lifetime));
        }

        static void AddForwardDataSource<T>(IServiceCollection services, ServiceLifetime lifetime, object serviceKey)
        {
            services.TryAdd(
                new ServiceDescriptor(
                    typeof(T),
                    serviceKey,
                    static (sp, key) => GetService<ClickHouseDataSource>(sp, key),
                    lifetime));
        }

        static T GetService<T>(IServiceProvider serviceProvider, object serviceKey) => serviceKey == null ? serviceProvider.GetRequiredService<T>() : serviceProvider.GetRequiredKeyedService<T>(serviceKey);
    }
}
#endif
