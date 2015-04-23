using System;
using System.Linq;
using System.Threading;

using Composite.Core;
using Composite.Core.Threading;
using Composite.Data;

using Hangfire.CompositeC1.Types;
using Hangfire.Server;

namespace Hangfire.CompositeC1
{
    public class ExpirationManager : IServerComponent
    {
        private static readonly TimeSpan DelayBetweenPasses = TimeSpan.FromSeconds(1);
        private const int NumberOfRecordsInSinglePass = 1000;

        private static readonly Type[] Types =
        {
            typeof(IAggregatedCounter),
            typeof(IJob),
            typeof(IList),
            typeof(ISet),
            typeof(IHash)
        };

        private readonly TimeSpan _checkInterval;

        public ExpirationManager(TimeSpan checkInterval)
        {
            _checkInterval = checkInterval;
        }

        public void Execute(CancellationToken cancellationToken)
        {
            var now = DateTime.UtcNow;

            using (ThreadDataManager.EnsureInitialize())
            {
                foreach (var t in Types)
                {
                    Log.LogVerbose("Hangfire", String.Format("Removing outdated records from type '{0}'", t.Name));

                    int removedCount;

                    do
                    {
                        var table = DataFacade.GetData(t).Cast<IExpirable>();
                        var data = (from d in table
                                    where d.ExpireAt < now
                                    orderby d.ExpireAt
                                    select d).Take(NumberOfRecordsInSinglePass).ToList();

                        removedCount = data.Count;

                        if (removedCount <= 0)
                        {
                            continue;
                        }

                        Log.LogVerbose("Hangfire", String.Format("Removed '{0}' outdated records from type '{1}'", removedCount, t.Name));

                        DataFacade.Delete<IData>(data);

                        cancellationToken.WaitHandle.WaitOne(DelayBetweenPasses);
                        cancellationToken.ThrowIfCancellationRequested();
                    } while (removedCount != 0);
                }
            }

            cancellationToken.WaitHandle.WaitOne(_checkInterval);
        }

        public override string ToString()
        {
            return "Composite C1 Records Expiration Manager";
        }
    }
}
