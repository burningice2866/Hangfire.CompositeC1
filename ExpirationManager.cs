using System;
using System.Linq;
using System.Threading;

using Composite;
using Composite.Data;

using Hangfire.Common;
using Hangfire.CompositeC1.Types;
using Hangfire.Logging;
using Hangfire.Server;

namespace Hangfire.CompositeC1
{
    public class ExpirationManager : IServerComponent, IBackgroundProcess
    {
        private static readonly TimeSpan DefaultLockTimeout = TimeSpan.FromMinutes(5);
        private const int NumberOfRecordsInSinglePass = 1000;

        private static readonly Type[] ExpirableTypes = (from type in typeof(CompositeC1Storage).Assembly.GetTypes()
                                                         where type.IsInterface
                                                            && type.Namespace != null && type.Namespace.Equals("Hangfire.CompositeC1.Types")
                                                            && typeof(IExpirable).IsAssignableFrom(type)
                                                         select type).ToArray();

        private readonly ILog _logger = LogProvider.GetCurrentClassLogger();

        private readonly CompositeC1Storage _storage;
        private readonly TimeSpan _checkInterval;

        public ExpirationManager(CompositeC1Storage storage, TimeSpan checkInterval)
        {
            Verify.ArgumentNotNull(storage, nameof(storage));

            _storage = storage;
            _checkInterval = checkInterval;
        }

        public void Execute(BackgroundProcessContext context)
        {
            Execute(context.CancellationToken);
        }

        public void Execute(CancellationToken cancellationToken)
        {
            var now = DateTime.UtcNow;

            foreach (var t in ExpirableTypes)
            {
                _logger.Debug($"Removing outdated records from type '{t.Name}'");

                _storage.UseConnection(connection =>
                {
                    int affected;

                    do
                    {
                        using (connection.AcquireDistributedLock("locks:ExpirationManager", DefaultLockTimeout))
                        {
                            var table = connection.Get<IExpirable>(t);
                            var records = (from d in table
                                           where d.ExpireAt < now
                                           orderby d.ExpireAt
                                           select d).Take(NumberOfRecordsInSinglePass).ToList();

                            affected = records.Count;

                            connection.Delete<IData>(records);
                        }
                    } while (affected == NumberOfRecordsInSinglePass);
                });
            }

            cancellationToken.Wait(_checkInterval);
        }

        public override string ToString()
        {
            return GetType().ToString();
        }
    }
}
