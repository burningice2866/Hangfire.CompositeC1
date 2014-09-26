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
    public class CompositeC1Connection : IStorageConnection
    {
        private static object FetchJobsLock = new object();

        private readonly DataConnection _connection;

        public CompositeC1Connection()
        {
            _connection = new DataConnection();
        }

        public IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
        {
            return SimpleLock.AcquireLock(resource, timeout);
        }

        public void AnnounceServer(string serverId, ServerContext context)
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

        public string CreateExpiredJob(Job job, IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn)
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

        public IWriteOnlyTransaction CreateWriteTransaction()
        {
            return new CompositeC1WriteOnlyTransaction();
        }

        public IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
        {
            Verify.ArgumentNotNull(queues, "queues");
            Verify.ArgumentCondition(queues.Length > 0, "queues", "Queues cannot be empty");

            IJobQueue queue;

            while (true)
            {
                var timeout = DateTime.UtcNow.Add(TimeSpan.FromMinutes(30).Negate());

                lock (FetchJobsLock)
                {
                    queue = (from q in _connection.Get<IJobQueue>()
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

        public Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            Verify.ArgumentNotNull(key, "key");

            var hashes = _connection.Get<IHash>().Where(h => h.Key == key).ToDictionary(h => h.Field, h => h.Value);

            return hashes.Count == 0 ? null : hashes;
        }

        public HashSet<string> GetAllItemsFromSet(string key)
        {
            Verify.ArgumentNotNull(key, "key");

            var values = _connection.Get<ISet>().Where(s => s.Key == key).Select(s => s.Value);

            return new HashSet<string>(values);
        }

        public string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
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

        public JobData GetJobData(string jobId)
        {
            Verify.ArgumentNotNull(jobId, "jobId");

            var jobData = _connection.Get<IJob>().SingleOrDefault(j => j.Id == Guid.Parse(jobId));
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

        public string GetJobParameter(string id, string name)
        {
            Verify.ArgumentNotNull(id, "id");
            Verify.ArgumentNotNull(name, "name");

            var job = _connection.Get<IJobParameter>().SingleOrDefault(p => p.JobId == Guid.Parse(id) && p.Name == name);

            return job == null ? null : job.Value;
        }

        public StateData GetStateData(string jobId)
        {
            Verify.ArgumentNotNull(jobId, "jobId");

            var state = (from job in _connection.Get<IJob>().Where(j => j.Id == Guid.Parse(jobId))
                         join s in _connection.Get<IState>() on job.StateId equals s.Id
                         select s).SingleOrDefault();

            if (state == null)
            {
                return null;
            }

            return new StateData
            {
                Name = state.Name,
                Reason = state.Reason,
                Data = JobHelper.FromJson<Dictionary<string, string>>(state.Data)
            };
        }

        public void Heartbeat(string serverId)
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

        public void RemoveServer(string serverId)
        {
            Verify.ArgumentNotNull(serverId, "serverId");

            var server = _connection.Get<IServer>().SingleOrDefault(s => s.Id == serverId);
            if (server != null)
            {
                _connection.Delete(server);
            }
        }

        public int RemoveTimedOutServers(TimeSpan timeOut)
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

        public void SetJobParameter(string id, string name, string value)
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

        public void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
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

                        hash.Key = key;
                        hash.Field = kvp.Key;
                    }

                    hash.Value = kvp.Value;

                    _connection.AddOrUpdate(add, hash);
                }

                transaction.Complete();
            }
        }

        public void Dispose()
        {
            _connection.Dispose();
        }
    }
}
