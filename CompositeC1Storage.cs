using System.Collections.Generic;

using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.CompositeC1
{
    public class CompositeC1Storage : JobStorage
    {
        private readonly CompositeC1StorageOptions _options;

        public CompositeC1Storage() : this(new CompositeC1StorageOptions()) { }

        public CompositeC1Storage(CompositeC1StorageOptions options)
        {
            _options = options;
        }

        public override IStorageConnection GetConnection()
        {
            return new CompositeC1Connection();
        }

        public override IMonitoringApi GetMonitoringApi()
        {
            return new CompositeC1MonitoringApi(this);
        }

        public override IEnumerable<IServerComponent> GetComponents()
        {
            return new IServerComponent[]
            {
                new ExpirationManager(this, _options.JobExpirationCheckInterval),
                new CountersAggregator(this, _options.CountersAggregateInterval)
            };
        }

        public override void WriteOptionsToLog(ILog logger)
        {
            logger.Info("Using the following options for Composite C1 job storage:");

            logger.InfoFormat("    Job expiration check interval: {0}.", _options.JobExpirationCheckInterval);
            logger.InfoFormat("    Counters aggregate interval: {0}.", _options.CountersAggregateInterval);

            base.WriteOptionsToLog(logger);
        }
    }
}
