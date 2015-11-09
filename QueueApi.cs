using System;
using System.Collections.Generic;
using System.Linq;

using Hangfire.CompositeC1.Entities;
using Hangfire.CompositeC1.Types;

namespace Hangfire.CompositeC1
{
    public static class QueueApi
    {
        public static IEnumerable<Guid> GetEnqueuedJobIds(CompositeC1Storage storage, string queue, int from, int perPage)
        {
            using (var data = (CompositeC1Connection)storage.GetConnection())
            {
                var jobs = data.Get<IJob>();
                var states = data.Get<IState>();
                var queues = data.Get<IJobQueue>();

                var ids = (from q in queues
                           join j in jobs on q.JobId equals j.Id
                           join s in states on j.StateId equals s.Id
                           where q.Queue == queue && !q.FetchedAt.HasValue
                           select j.Id).Skip(from).Take(perPage).ToList();

                return ids;
            }
        }

        public static IEnumerable<Guid> GetFetchedJobIds(CompositeC1Storage storage, string queue, int from, int perPage)
        {
            using (var data = (CompositeC1Connection)storage.GetConnection())
            {
                var jobs = data.Get<IJob>();
                var states = data.Get<IState>();
                var queues = data.Get<IJobQueue>();

                var ids = (from q in queues
                           join j in jobs on q.JobId equals j.Id
                           join s in states on j.StateId equals s.Id
                           where q.Queue == queue && q.FetchedAt.HasValue
                           select j.Id).Skip(from).Take(perPage).ToList();

                return ids;
            }
        }

        public static EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(CompositeC1Storage storage, string queue)
        {
            using (var data = (CompositeC1Connection)storage.GetConnection())
            {
                var fetchedCount = data.Get<IJobQueue>().Count(q => q.Queue == queue && q.FetchedAt.HasValue);
                var enqueuedCount = data.Get<IJobQueue>().Count(q => q.Queue == queue && !q.FetchedAt.HasValue);

                return new EnqueuedAndFetchedCountDto
                {
                    EnqueuedCount = enqueuedCount,
                    FetchedCount = fetchedCount
                };
            }
        }
    }
}
