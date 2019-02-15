using System;
using System.Collections.Generic;
using System.Linq;

using Composite;

using Hangfire.CompositeC1.Entities;
using Hangfire.CompositeC1.Types;

namespace Hangfire.CompositeC1
{
    public class QueueApi
    {
        private readonly CompositeC1Storage _storage;

        public QueueApi(CompositeC1Storage storage)
        {
            Verify.ArgumentNotNull(storage, "storage");

            _storage = storage;
        }

        public IEnumerable<string> GetQueues()
        {
            return _storage.UseConnection(connection =>
            {
                return connection.Get<IJobQueue>().Select(j => j.Queue).Distinct();
            });
        }

        public IEnumerable<Guid> GetEnqueuedJobIds(string queue, int from, int perPage)
        {
            return _storage.UseConnection(connection =>
            {
                var queues = connection.Get<IJobQueue>();

                var jobIds = (from q in queues
                              where q.Queue == queue
                              select q.JobId).Skip(from).Take(perPage).ToList();

                return jobIds;
            });
        }

        public IEnumerable<Guid> GetFetchedJobIds(string queue, int from, int perPage)
        {
            return _storage.UseConnection(connection =>
            {
                var jobs = connection.Get<IJob>();
                var queues = connection.Get<IJobQueue>();

                var ids = (from q in queues
                           join j in jobs on q.JobId equals j.Id
                           where q.Queue == queue && q.FetchedAt.HasValue
                           select j.Id).Skip(from).Take(perPage).ToList();

                return ids;
            });
        }

        public EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue)
        {
            return _storage.UseConnection(connection =>
            {
                var jobs = connection.Get<IJobQueue>().Where(q => q.Queue == queue);

                var fetchedCount = jobs.Count(q => q.FetchedAt.HasValue);
                var enqueuedCount = jobs.Count(q => !q.FetchedAt.HasValue);

                return new EnqueuedAndFetchedCountDto
                {
                    EnqueuedCount = enqueuedCount,
                    FetchedCount = fetchedCount
                };
            });
        }
    }
}
