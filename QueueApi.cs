using System;
using System.Collections.Generic;
using System.Linq;

using Composite.Data;

using Hangfire.CompositeC1.Entities;
using Hangfire.CompositeC1.Types;

namespace Hangfire.CompositeC1
{
    public static class QueueApi
    {
        public static IEnumerable<Guid> GetEnqueuedJobIds(string queue, int from, int perPage)
        {
            using (var data = new DataConnection())
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

        public static IEnumerable<Guid> GetFetechedJobIds(string queue, int from, int perPage)
        {
            using (var data = new DataConnection())
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

        public static EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue)
        {
            using (var data = new DataConnection())
            {
                var fetechedCount = data.Get<IJobQueue>().Count(q => q.Queue == queue && q.FetchedAt.HasValue);
                var enqueuedCount = data.Get<IJobQueue>().Count(q => q.Queue == queue && !q.FetchedAt.HasValue);

                return new EnqueuedAndFetchedCountDto
                {
                    EnqueuedCount = fetechedCount,
                    FetchedCount = enqueuedCount
                };
            }
        }
    }
}
