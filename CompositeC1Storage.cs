using System;
using System.Collections.Generic;
using System.Linq;

using Composite.Data;
using Composite.Data.DynamicTypes;

using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.CompositeC1
{
    public class CompositeC1Storage : JobStorage
    {
        public CompositeC1StorageOptions Options { get; }

        public CompositeC1Storage() : this(new CompositeC1StorageOptions()) { }

        public CompositeC1Storage(CompositeC1StorageOptions options)
        {
            Options = options;

            EnsureDataTypes();
        }

        public override IStorageConnection GetConnection()
        {
            return new CompositeC1Connection(this);
        }

        public void UseConnection(Action<CompositeC1Connection> action)
        {
            using (var data = (CompositeC1Connection)GetConnection())
            {
                action(data);
            }
        }

        public T UseConnection<T>(Func<CompositeC1Connection, T> func)
        {
            using (var data = (CompositeC1Connection)GetConnection())
            {
                return func(data);
            }
        }

        public override IMonitoringApi GetMonitoringApi()
        {
            return new CompositeC1MonitoringApi(this);
        }

        public override IEnumerable<IServerComponent> GetComponents()
        {
            return new IServerComponent[]
            {
                new ExpirationManager(this, Options.JobExpirationCheckInterval),
                new CountersAggregator(this, Options.CountersAggregateInterval)
            };
        }

        public override void WriteOptionsToLog(ILog logger)
        {
            logger.Info("Using the following options for Composite C1 job storage:");

            logger.InfoFormat("    Job expiration check interval: {0}.", Options.JobExpirationCheckInterval);
            logger.InfoFormat("    Counters aggregate interval: {0}.", Options.CountersAggregateInterval);

            if (Options.DashboardJobListLimit.HasValue)
            {
                logger.InfoFormat("    Dashboard Joblist limit: {0}.", Options.DashboardJobListLimit.Value);
            }

            base.WriteOptionsToLog(logger);
        }

        private static void EnsureDataTypes()
        {
            var types = from type in typeof(CompositeC1Storage).Assembly.GetTypes()
                        where type.IsInterface
                            && type.Namespace != null && type.Namespace.Equals("Hangfire.CompositeC1.Types")
                            && typeof(IData).IsAssignableFrom(type)
                        let attributes = type.GetCustomAttributes(typeof(ImmutableTypeIdAttribute), false)
                        where attributes.Any()
                        select type;

            foreach (var t in types)
            {
                DynamicTypeManager.EnsureCreateStore(t);
            }
        }
    }
}
