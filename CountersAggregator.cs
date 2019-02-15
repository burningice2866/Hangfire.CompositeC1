using System;
using System.Linq;
using System.Threading;

using Composite;

using Hangfire.Common;
using Hangfire.CompositeC1.Types;
using Hangfire.Logging;
using Hangfire.Server;

namespace Hangfire.CompositeC1
{
    public class CountersAggregator : IServerComponent, IBackgroundProcess
    {
        private const int NumberOfRecordsInSinglePass = 10000;
        private static readonly TimeSpan DelayBetweenPasses = TimeSpan.FromMilliseconds(500);

        private readonly ILog _logger = LogProvider.GetCurrentClassLogger();

        private readonly CompositeC1Storage _storage;
        private readonly TimeSpan _interval;

        public CountersAggregator(CompositeC1Storage storage, TimeSpan interval)
        {
            Verify.ArgumentNotNull(storage, "storage");

            _storage = storage;
            _interval = interval;
        }

        public void Execute(BackgroundProcessContext context)
        {
            Execute(context.CancellationToken);
        }

        public void Execute(CancellationToken cancellationToken)
        {
            _logger.Debug("Aggregating records in 'Counter' table...");

            int removedCount;

            do
            {
                removedCount = _storage.UseConnection(connection =>
                {
                    var counters = connection.Get<ICounter>().Take(NumberOfRecordsInSinglePass).ToList();

                    var groupedCounters = counters.GroupBy(c => c.Key).Select(g => new
                    {
                        g.Key,
                        Value = g.Sum(c => c.Value),
                        ExpireAt = g.Max(c => c.ExpireAt)
                    });

                    foreach (var counter in groupedCounters)
                    {
                        var aggregate = connection.Get<IAggregatedCounter>().SingleOrDefault(a => a.Key == counter.Key);
                        if (aggregate == null)
                        {
                            aggregate = connection.CreateNew<IAggregatedCounter>();

                            aggregate.Id = Guid.NewGuid();
                            aggregate.Key = counter.Key;
                            aggregate.Value = counter.Value;
                            aggregate.ExpireAt = counter.ExpireAt;
                        }
                        else
                        {
                            aggregate.Value += counter.Value;

                            if (counter.ExpireAt > aggregate.ExpireAt)
                            {
                                aggregate.ExpireAt = counter.ExpireAt;
                            }
                        }

                        connection.AddOrUpdate(aggregate);
                    }

                    connection.Delete<ICounter>(counters);

                    return counters.Count();
                });

                if (removedCount > 0)
                {
                    cancellationToken.Wait(DelayBetweenPasses);
                    cancellationToken.ThrowIfCancellationRequested();
                }
            } while (removedCount != 0);

            _logger.Trace("Records from the 'Counter' table aggregated.");

            cancellationToken.Wait(_interval);
        }

        public override string ToString()
        {
            return GetType().ToString();
        }
    }
}
