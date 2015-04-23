using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Transactions;

using Composite;
using Composite.Data;

using Hangfire.Common;
using Hangfire.CompositeC1.Types;
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.CompositeC1
{
    public class CompositeC1Connection : JobStorageConnection
    {
        private static readonly object FetchJobsLock = new object();

        private readonly DataConnection _connection;

        public CompositeC1Connection()
        {
            _connection = new DataConnection();
        }

        public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
        {
            return SimpleLock.AcquireLock(resource, timeout);
        }

        public override void AnnounceServer(string serverId, ServerContext context)
        {
            Verify.ArgumentNotNull(serverId, "serverId");
            Verify.ArgumentNotNull(context, "context");

            var add = false;

            var server = _connection.Get<IServer>().SingleOrDefault(s => s.Id == serverId);
            if (server == null)
            {
                add = true;

                server = _connection.CreateNew<IServer>();

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

            _connection.AddOrUpdate(add, server);
        }

        public override string CreateExpiredJob(Job job, IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn)
        {
            Verify.ArgumentNotNull(job, "job");
            Verify.ArgumentNotNull(parameters, "parameters");

            var invocationData = InvocationData.Serialize(job);

            var jobData = _connection.CreateNew<IJob>();

            jobData.Id = Guid.NewGuid();
            jobData.InvocationData = JobHelper.ToJson(invocationData);
            jobData.Arguments = invocationData.Arguments;
            jobData.CreatedAt = createdAt;
            jobData.ExpireAt = createdAt.Add(expireIn);

            _connection.Add(jobData);

            if (parameters.Count > 0)
            {
                var list = new List<IJobParameter>();

                foreach (var kvp in parameters)
                {
                    var parametersData = _connection.CreateNew<IJobParameter>();

                    parametersData.Id = Guid.NewGuid();
                    parametersData.JobId = jobData.Id;
                    parametersData.Name = kvp.Key;
                    parametersData.Value = kvp.Value;

                    list.Add(parametersData);
                }

                _connection.Add<IJobParameter>(list);
            }

            return jobData.Id.ToString();
        }

        public override IWriteOnlyTransaction CreateWriteTransaction()
        {
            return new CompositeC1WriteOnlyTransaction();
        }

        public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
        {
            Verify.ArgumentNotNull(queues, "queues");
            Verify.ArgumentCondition(queues.Length > 0, "queues", "Queues cannot be empty");

            IJobQueue queue;

            while (true)
            {
                var timeout = DateTime.UtcNow.Add(TimeSpan.FromMinutes(30).Negate());

                lock (FetchJobsLock)
                {
                    var jobQueues = _connection.Get<IJobQueue>();

                    queue = (from q in jobQueues
                             where queues.Contains(q.Queue)
                                   && (!q.FetchedAt.HasValue || q.FetchedAt.Value < timeout)
                             orderby q.AddedAt descending
                             select q).FirstOrDefault();

                    if (queue != null)
                    {
                        queue.FetchedAt = DateTime.UtcNow;

                        _connection.Update(queue);

                        break;
                    }
                }

                cancellationToken.WaitHandle.WaitOne(TimeSpan.FromSeconds(15));
                cancellationToken.ThrowIfCancellationRequested();
            }

            return new CompositeC1FetchedJob(_connection, queue);
        }

        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            Verify.ArgumentNotNull(key, "key");

            var hashes = _connection.Get<IHash>().Where(h => h.Key == key).ToDictionary(h => h.Field, h => h.Value);

            return hashes.Count == 0 ? null : hashes;
        }

        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            Verify.ArgumentNotNull(key, "key");

            var values = _connection.Get<ISet>().Where(s => s.Key == key).Select(s => s.Value);

            return new HashSet<string>(values);
        }

        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
        {
            Verify.ArgumentNotNull(key, "key");

            if (toScore < fromScore)
            {
                throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.");
            }

            var set =
                _connection.Get<ISet>()
                    .Where(s => s.Key == key && s.Score >= fromScore && s.Score <= toScore)
                    .OrderByDescending(s => s.Score)
                    .FirstOrDefault();

            return set == null ? null : set.Value;
        }

        public override JobData GetJobData(string jobId)
        {
            Verify.ArgumentNotNull(jobId, "jobId");

            Guid id;
            if (!Guid.TryParse(jobId, out id))
            {
                return null;
            }

            var jobData = _connection.Get<IJob>().SingleOrDefault(j => j.Id == id);
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
            Verify.ArgumentNotNull(id, "id");
            Verify.ArgumentNotNull(name, "name");

            var job = _connection.Get<IJobParameter>().SingleOrDefault(p => p.JobId == Guid.Parse(id) && p.Name == name);

            return job == null ? null : job.Value;
        }

        public override long GetListCount(string key)
        {
            Verify.ArgumentNotNull(key, "key");

            return _connection.Get<IList>().Count(l => l.Key == key);
        }

        public override long GetSetCount(string key)
        {
            Verify.ArgumentNotNull(key, "key");

            return _connection.Get<ISet>().Count(s => s.Key == key);
        }

        public override StateData GetStateData(string jobId)
        {
            Verify.ArgumentNotNull(jobId, "jobId");

            Guid id;
            if (!Guid.TryParse(jobId, out id))
            {
                return null;
            }

            var jobs = _connection.Get<IJob>();
            var states = _connection.Get<IState>();

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
            Verify.ArgumentNotNull(key, "key");
            Verify.ArgumentNotNull(name, "name");

            return _connection
                .Get<IHash>()
                .Where(h => h.Key == key && h.Field == name)
                .Select(h => h.Value).SingleOrDefault();
        }

        public override List<string> GetAllItemsFromList(string key)
        {
            Verify.ArgumentNotNull(key, "key");

            return _connection
                .Get<IList>()
                .Where(l => l.Key == key)
                .OrderBy(l => l.Id)
                .Select(l => l.Value)
                .ToList();
        }

        public override long GetCounter(string key)
        {
            Verify.ArgumentNotNull(key, "key");

            return _connection.GetCombinedCounter(key) ?? 0;
        }

        public override long GetHashCount(string key)
        {
            Verify.ArgumentNotNull(key, "key");

            return _connection.Get<IHash>().Count(h => h.Key == key);
        }

        public override TimeSpan GetHashTtl(string key)
        {
            Verify.ArgumentNotNull(key, "key");

            var date = _connection.Get<IHash>().Select(h => h.ExpireAt).Min();

            return date.HasValue ? date.Value - DateTime.UtcNow : TimeSpan.FromSeconds(-1);
        }

        public override TimeSpan GetListTtl(string key)
        {
            Verify.ArgumentNotNull(key, "key");

            var date = _connection.Get<IList>().Select(l => l.ExpireAt).Min();

            return date.HasValue ? date.Value - DateTime.UtcNow : TimeSpan.FromSeconds(-1);
        }

        public override TimeSpan GetSetTtl(string key)
        {
            Verify.ArgumentNotNull(key, "key");

            var date = _connection.Get<ISet>().Select(l => l.ExpireAt).Min();

            return date.HasValue ? date.Value - DateTime.UtcNow : TimeSpan.FromSeconds(-1);
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            Verify.ArgumentNotNull(key, "key");

            var count = endingAt - startingFrom;

            return _connection
                .Get<IList>()
                .Where(l => l.Key == key)
                .OrderBy(l => l.Id)
                .Skip(startingFrom)
                .Take(count)
                .Select(l => l.Value)
                .ToList();
        }

        public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
        {
            Verify.ArgumentNotNull(key, "key");

            var count = endingAt - startingFrom;

            return _connection
                .Get<ISet>()
                .Where(s => s.Key == key)
                .OrderBy(s => s.Id)
                .Skip(startingFrom)
                .Take(count)
                .Select(s => s.Value)
                .ToList();
        }

        public override void Heartbeat(string serverId)
        {
            Verify.ArgumentNotNull(serverId, "serverId");

            var server = _connection.Get<IServer>().SingleOrDefault(s => s.Id == serverId);
            if (server == null)
            {
                return;
            }

            server.LastHeartbeat = DateTime.UtcNow;

            _connection.Update(server);
        }

        public override void RemoveServer(string serverId)
        {
            Verify.ArgumentNotNull(serverId, "serverId");

            var server = _connection.Get<IServer>().SingleOrDefault(s => s.Id == serverId);
            if (server != null)
            {
                _connection.Delete(server);
            }
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            if (timeOut.Duration() != timeOut)
            {
                throw new ArgumentException("The `timeOut` value must be positive.", "timeOut");
            }

            var timeOutAt = DateTime.UtcNow.Add(timeOut.Negate());
            var servers = _connection.Get<IServer>().Where(s => s.LastHeartbeat < timeOutAt).ToList();

            _connection.Delete<IServer>(servers);

            return servers.Count;
        }

        public override void SetJobParameter(string id, string name, string value)
        {
            Verify.ArgumentNotNull(id, "id");
            Verify.ArgumentNotNull(name, "name");

            var add = false;

            var parameter = _connection.Get<IJobParameter>().SingleOrDefault(s => s.JobId == Guid.Parse(id) && s.Name == name);
            if (parameter == null)
            {
                add = true;

                parameter = _connection.CreateNew<IJobParameter>();

                parameter.Id = Guid.NewGuid();
                parameter.JobId = Guid.Parse(id);
                parameter.Name = name;
            }

            parameter.Value = value;

            _connection.AddOrUpdate(add, parameter);
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            Verify.ArgumentNotNull(key, "key");
            Verify.ArgumentNotNull(keyValuePairs, "keyValuePairs");

            using (var transaction = new TransactionScope())
            {
                foreach (var kvp in keyValuePairs)
                {
                    var add = false;

                    var hash = _connection.Get<IHash>().SingleOrDefault(h => h.Key == key && h.Field == kvp.Key);
                    if (hash == null)
                    {
                        add = true;

                        hash = _connection.CreateNew<IHash>();

                        hash.Id = Guid.NewGuid();
                        hash.Key = key;
                        hash.Field = kvp.Key;
                    }

                    hash.Value = kvp.Value;

                    _connection.AddOrUpdate(add, hash);
                }

                transaction.Complete();
            }
        }

        public override void Dispose()
        {
            _connection.Dispose();
        }
    }
}
