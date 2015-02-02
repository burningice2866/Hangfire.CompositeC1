﻿using System.Collections.Generic;

using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.CompositeC1
{
    public class CompositeC1Storage : JobStorage
    {
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
            return new[] { new ExpirationManager() };
        }
    }
}
