﻿using System;
using System.Linq;
using System.Threading;

using Composite.Data;

using Hangfire.CompositeC1.Types;
using Hangfire.Server;

namespace Hangfire.CompositeC1
{
    public class CountersAggregator : IServerComponent
    {
        private const int NumberOfRecordsInSinglePass = 1000;
        private static readonly TimeSpan DelayBetweenPasses = TimeSpan.FromSeconds(1);

        private readonly TimeSpan _interval;

        public CountersAggregator(TimeSpan interval)
        {
            _interval = interval;
        }

        public void Execute(CancellationToken cancellationToken)
        {
            var removedCount = 0;

            do
            {
                using (var data = new DataConnection())
                {
                    var counters = data.Get<ICounter>().Take(NumberOfRecordsInSinglePass).ToList();

                    var groupedCounters = counters.GroupBy(c => c.Key).Select(g => new
                    {
                        g.Key,
                        Value = g.Sum(c => c.Value),
                        ExpireAt = g.Max(c => c.ExpireAt)
                    });

                    foreach (var counter in groupedCounters)
                    {
                        var add = false;
                        var aggregate = data.Get<IAggregatedCounter>().SingleOrDefault(a => a.Key == counter.Key);

                        if (aggregate == null)
                        {
                            aggregate = data.CreateNew<IAggregatedCounter>();

                            aggregate.Id = Guid.NewGuid();
                            aggregate.Key = counter.Key;
                            aggregate.Value = counter.Value;
                            aggregate.ExpireAt = counter.ExpireAt;

                            add = true;
                        }
                        else
                        {
                            aggregate.Value += counter.Value;

                            if (counter.ExpireAt > aggregate.ExpireAt)
                            {
                                aggregate.ExpireAt = counter.ExpireAt;
                            }
                        }

                        data.AddOrUpdate(add, aggregate);
                    }

                    removedCount = counters.Count();

                    data.Delete<ICounter>(counters);
                }

                if (removedCount > 0)
                {
                    cancellationToken.WaitHandle.WaitOne(DelayBetweenPasses);
                    cancellationToken.ThrowIfCancellationRequested();
                }
            } while (removedCount != 0);

            cancellationToken.WaitHandle.WaitOne(_interval);
        }

        public override string ToString()
        {
            return "Counter Table Aggregator";
        }
    }
}