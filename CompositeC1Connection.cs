using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

using Composite;
using Composite.Data;

using Hangfire.Common;
using Hangfire.CompositeC1.Types;
using Hangfire.Server;
using Hangfire.Storage;

using IList = Hangfire.CompositeC1.Types.IList;

namespace Hangfire.CompositeC1
{
    public class CompositeC1Connection : JobStorageConnection
    {
        private static readonly object FetchJobsLock = new object();

        private readonly CompositeC1Storage _storage;

        // This is an optimization that helps to overcome the polling delay, when
        // both client and server reside in the same process. Everything is working
        // without this event, but it helps to reduce the delays in processing.
        private readonly AutoResetEvent _newItemInQueueEvent = new AutoResetEvent(true);

        private readonly DataConnection _connection;

        public CompositeC1Connection(CompositeC1Storage storage)
        {
            _storage = storage;
            _connection = new DataConnection();
        }

        public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
        {
            return SimpleLock.AcquireLock(resource, timeout);
        }

        public override void AnnounceServer(string serverId, ServerContext context)
        {
            Verify.ArgumentNotNull(serverId, nameof(serverId));
            Verify.ArgumentNotNull(context, nameof(context));

            var server = Get<IServer>().SingleOrDefault(s => s.Id == serverId);
            if (server == null)
            {
                server = CreateNew<IServer>();

                server.Id = serverId;
            }

            var data = new
            {
                context.WorkerCount,
                context.Queues,
                StartedAt = DateTime.UtcNow
            };

            server.LastHeartbeat = DateTime.UtcNow;
            server.Data = JobHelper.ToJson(data);

            AddOrUpdate(server);
        }

        public override string CreateExpiredJob(Job job, IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn)
        {
            Verify.ArgumentNotNull(job, nameof(job));
            Verify.ArgumentNotNull(parameters, nameof(parameters));

            var invocationData = InvocationData.Serialize(job);

            var jobData = CreateNew<IJob>();

            jobData.Id = Guid.NewGuid();
            jobData.InvocationData = JobHelper.ToJson(invocationData);
            jobData.Arguments = invocationData.Arguments;
            jobData.CreatedAt = createdAt;
            jobData.ExpireAt = createdAt.Add(expireIn);

            Add(jobData);

            if (parameters.Count > 0)
            {
                var list = new List<IJobParameter>();

                foreach (var kvp in parameters)
                {
                    var parametersData = CreateNew<IJobParameter>();

                    parametersData.Id = Guid.NewGuid();
                    parametersData.JobId = jobData.Id;
                    parametersData.Name = kvp.Key;
                    parametersData.Value = kvp.Value;

                    list.Add(parametersData);
                }

                Add<IJobParameter>(list);
            }

            return jobData.Id.ToString();
        }

        public override IWriteOnlyTransaction CreateWriteTransaction()
        {
            return new CompositeC1WriteOnlyTransaction(this);
        }

        public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
        {
            Verify.ArgumentNotNull(queues, nameof(queues));
            Verify.ArgumentCondition(queues.Length > 0, nameof(queues), "Queue array must be non-empty.");

            IJobQueue fetchedJob;

            using (var cancellationEvent = cancellationToken.GetCancellationEvent())
            {
                do
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var timeout = DateTime.UtcNow.Add(_storage.Options.SlidingInvisibilityTimeout.Negate());

                    lock (FetchJobsLock)
                    {
                        var queuedJobs = Get<IJobQueue>();

                        fetchedJob = (from q in queuedJobs
                                      where queues.Contains(q.Queue) &&
                                            (!q.FetchedAt.HasValue || q.FetchedAt.Value < timeout)
                                      orderby q.AddedAt descending
                                      select q).FirstOrDefault();

                        if (fetchedJob != null)
                        {
                            fetchedJob.FetchedAt = DateTime.UtcNow;

                            Update(fetchedJob);

                            break;
                        }
                    }

                    WaitHandle.WaitAny(new WaitHandle[] { cancellationEvent.WaitHandle, _newItemInQueueEvent }, _storage.Options.QueuePollInterval);
                    cancellationToken.ThrowIfCancellationRequested();
                } while (true);
            }

            return new QueuedJob(_storage, fetchedJob.Id, fetchedJob.JobId.ToString(), fetchedJob.Queue, fetchedJob.FetchedAt);
        }

        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            Verify.ArgumentNotNull(key, nameof(key));

            var hashes = (from hash in Get<IHash>()
                          where hash.Key == key
                          select new { hash.Field, hash.Value }).ToDictionary(h => h.Field, h => h.Value);

            return hashes.Count == 0 ? null : hashes;
        }

        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            Verify.ArgumentNotNull(key, nameof(key));

            var values = Get<ISet>().Where(s => s.Key == key).Select(s => s.Value);

            return new HashSet<string>(values);
        }

        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
        {
            Verify.ArgumentNotNull(key, nameof(key));

            if (toScore < fromScore)
            {
                throw new ArgumentException($"The `{nameof(toScore)}` value must be higher or equal to the `{nameof(fromScore)}` value.");
            }

            var value = (from set in Get<ISet>()
                         where set.Key == key && set.Score >= fromScore && set.Score <= toScore
                         orderby set.Score descending
                         select set.Value).FirstOrDefault();

            return value;
        }

        public override JobData GetJobData(string jobId)
        {
            Verify.ArgumentNotNull(jobId, nameof(jobId));

            if (!Guid.TryParse(jobId, out var id))
            {
                return null;
            }

            var jobData = Get<IJob>().SingleOrDefault(j => j.Id == id);
            if (jobData == null)
            {
                return null;
            }

            var invocationData = JobHelper.FromJson<InvocationData>(jobData.InvocationData);

            invocationData.Arguments = jobData.Arguments;

            Job job = null;
            JobLoadException loadException = null;

            try
            {
                job = invocationData.Deserialize();
            }
            catch (JobLoadException ex)
            {
                loadException = ex;
            }

            return new JobData
            {
                Job = job,
                State = jobData.StateName,
                CreatedAt = jobData.CreatedAt,
                LoadException = loadException
            };
        }

        public override string GetJobParameter(string id, string name)
        {
            Verify.ArgumentNotNull(id, nameof(id));
            Verify.ArgumentNotNull(name, nameof(name));

            var job = Get<IJobParameter>().SingleOrDefault(p => p.JobId == Guid.Parse(id) && p.Name == name);

            return job?.Value;
        }

        public override long GetListCount(string key)
        {
            Verify.ArgumentNotNull(key, nameof(key));

            return Get<IList>().Count(l => l.Key == key);
        }

        public override long GetSetCount(string key)
        {
            Verify.ArgumentNotNull(key, nameof(key));

            return Get<ISet>().Count(s => s.Key == key);
        }

        public override StateData GetStateData(string jobId)
        {
            Verify.ArgumentNotNull(jobId, nameof(jobId));

            if (!Guid.TryParse(jobId, out var id))
            {
                return null;
            }

            var jobs = Get<IJob>();
            var states = Get<IState>();

            var state = (from job in jobs
                         where job.Id == id
                         join s in states on job.StateId equals s.Id
                         select s).SingleOrDefault();

            if (state == null)
            {
                return null;
            }

            return new StateData
            {
                Name = state.Name,
                Reason = state.Reason,
                Data = new Dictionary<string, string>(
                    JobHelper.FromJson<Dictionary<string, string>>(state.Data),
                    StringComparer.OrdinalIgnoreCase)
            };
        }

        public override string GetValueFromHash(string key, string name)
        {
            Verify.ArgumentNotNull(key, nameof(key));
            Verify.ArgumentNotNull(name, nameof(name));

            return Get<IHash>()
                .Where(h => h.Key == key && h.Field == name)
                .Select(h => h.Value).SingleOrDefault();
        }

        public override List<string> GetAllItemsFromList(string key)
        {
            Verify.ArgumentNotNull(key, nameof(key));

            return Get<IList>()
                .Where(l => l.Key == key)
                .OrderBy(l => l.Id)
                .Select(l => l.Value)
                .ToList();
        }

        public override long GetCounter(string key)
        {
            Verify.ArgumentNotNull(key, nameof(key));

            return GetCombinedCounter(key) ?? 0;
        }

        public override long GetHashCount(string key)
        {
            Verify.ArgumentNotNull(key, nameof(key));

            return Get<IHash>().Count(h => h.Key == key);
        }

        public override TimeSpan GetHashTtl(string key)
        {
            Verify.ArgumentNotNull(key, nameof(key));

            var date = Get<IHash>().Select(h => h.ExpireAt).Min();

            return date.HasValue ? date.Value - DateTime.UtcNow : TimeSpan.FromSeconds(-1);
        }

        public override TimeSpan GetListTtl(string key)
        {
            Verify.ArgumentNotNull(key, nameof(key));

            var date = Get<IList>().Select(l => l.ExpireAt).Min();

            return date.HasValue ? date.Value - DateTime.UtcNow : TimeSpan.FromSeconds(-1);
        }

        public override TimeSpan GetSetTtl(string key)
        {
            Verify.ArgumentNotNull(key, nameof(key));

            var date = Get<ISet>().Select(l => l.ExpireAt).Min();

            return date.HasValue ? date.Value - DateTime.UtcNow : TimeSpan.FromSeconds(-1);
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            Verify.ArgumentNotNull(key, nameof(key));

            var count = endingAt - startingFrom;

            return Get<IList>()
                .Where(l => l.Key == key)
                .OrderBy(l => l.Id)
                .Skip(startingFrom)
                .Take(count)
                .Select(l => l.Value)
                .ToList();
        }

        public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
        {
            Verify.ArgumentNotNull(key, nameof(key));

            var count = endingAt - startingFrom;

            return Get<ISet>()
                .Where(s => s.Key == key)
                .OrderBy(s => s.Id)
                .Skip(startingFrom)
                .Take(count)
                .Select(s => s.Value)
                .ToList();
        }

        public override void Heartbeat(string serverId)
        {
            Verify.ArgumentNotNull(serverId, nameof(serverId));

            var server = Get<IServer>().SingleOrDefault(s => s.Id == serverId);
            if (server == null)
            {
                return;
            }

            server.LastHeartbeat = DateTime.UtcNow;

            Update(server);
        }

        public override void RemoveServer(string serverId)
        {
            Verify.ArgumentNotNull(serverId, nameof(serverId));

            var server = Get<IServer>().SingleOrDefault(s => s.Id == serverId);
            if (server != null)
            {
                Delete(server);
            }
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            if (timeOut.Duration() != timeOut)
            {
                throw new ArgumentException("The `timeOut` value must be positive.", nameof(timeOut));
            }

            var timeOutAt = DateTime.UtcNow.Add(timeOut.Negate());
            var servers = Get<IServer>().Where(s => s.LastHeartbeat < timeOutAt).ToList();

            Delete<IServer>(servers);

            return servers.Count;
        }

        public override void SetJobParameter(string id, string name, string value)
        {
            Verify.ArgumentNotNull(id, nameof(id));
            Verify.ArgumentNotNull(name, nameof(name));

            var parameter = Get<IJobParameter>().SingleOrDefault(s => s.JobId == Guid.Parse(id) && s.Name == name);
            if (parameter == null)
            {
                parameter = CreateNew<IJobParameter>();

                parameter.Id = Guid.NewGuid();
                parameter.JobId = Guid.Parse(id);
                parameter.Name = name;
            }

            parameter.Value = value;

            AddOrUpdate(parameter);
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            Verify.ArgumentNotNull(key, nameof(key));
            Verify.ArgumentNotNull(keyValuePairs, nameof(keyValuePairs));

            using (var transaction = CreateWriteTransaction())
            {
                transaction.SetRangeInHash(key, keyValuePairs);

                transaction.Commit();
            }
        }

        public long? GetCombinedCounter(string key)
        {
            var counters = Get<ICounter>().Where(c => c.Key == key).Select(c => (long?)c.Value);
            var aggregatedCounters = Get<IAggregatedCounter>().Where(c => c.Key == key).Select(c => (long?)c.Value);

            return counters.Concat(aggregatedCounters).Sum(v => v);
        }

        public void NotifyNewItemInQueue()
        {
            _newItemInQueueEvent.Set();
        }

        public T CreateNew<T>() where T : class, IData
        {
            return _connection.CreateNew<T>();
        }

        public void AddOrUpdate<T>(T item) where T : class, IData
        {
            if (item.DataSourceId.ExistsInStore)
            {
                Update(item);
            }
            else
            {
                Add(item);
            }
        }

        public void Add<T>(T item) where T : class, IData
        {
            _connection.Add(item);
        }

        public void Add<T>(IEnumerable<T> items) where T : class, IData
        {
            _connection.Add(items);
        }

        public IQueryable<T> Get<T>() where T : class, IData
        {
            return _connection.Get<T>();
        }

        public IQueryable<T> Get<T>(Type t) where T : class, IData
        {
            return DataFacade.GetData(t).Cast<T>();
        }

        public void Update<T>(T item) where T : class, IData
        {
            _connection.Update(item);
        }

        public void Delete<T>(T item) where T : class, IData
        {
            _connection.Delete<IData>(item);
        }

        public void Delete<T>(IEnumerable<T> items) where T : class, IData
        {
            _connection.Delete<IData>(items);
        }

        public override void Dispose()
        {
            _connection.Dispose();
        }
    }
}
