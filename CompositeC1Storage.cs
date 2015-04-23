using System.Collections.Generic;

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
            return new CompositeC1MonitoringApi();
        }

        public override IEnumerable<IServerComponent> GetComponents()
        {
            return new IServerComponent[]
            {
                new ExpirationManager(_options.JobExpirationCheckInterval),
                new CountersAggregator(_options.CountersAggregateInterval)
            };
        }
    }
}
