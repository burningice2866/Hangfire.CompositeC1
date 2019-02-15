using System;
using System.Collections.Generic;
using System.Linq;
using System.Transactions;

using Composite;

using Hangfire.Common;
using Hangfire.CompositeC1.Types;
using Hangfire.Storage;

using IHangfireState = Hangfire.States.IState;

namespace Hangfire.CompositeC1
{
    public class CompositeC1WriteOnlyTransaction : IWriteOnlyTransaction
    {
        private readonly IList<Action<CompositeC1Connection>> _queue = new List<Action<CompositeC1Connection>>();

        private readonly CompositeC1Connection _connection;
        private bool _notifyNewItemInQueue;

        public CompositeC1WriteOnlyTransaction(CompositeC1Connection connection)
        {
            _connection = connection;
        }

        public void AddJobState(string jobId, IHangfireState state)
        {
            QueueCommand(connection =>
            {
                var stateData = connection.CreateNew<IState>();

                stateData.Id = Guid.NewGuid();
                stateData.JobId = Guid.Parse(jobId);
                stateData.Name = state.Name;
                stateData.Reason = state.Reason;
                stateData.CreatedAt = DateTime.UtcNow;
                stateData.Data = JobHelper.ToJson(state.SerializeData());

                connection.Add(stateData);
            });
        }

        public void AddToQueue(string queue, string jobId)
        {
            QueueCommand(connection =>
            {
                var queuedJob = connection.CreateNew<IJobQueue>();

                queuedJob.Id = Guid.NewGuid();
                queuedJob.JobId = Guid.Parse(jobId);
                queuedJob.Queue = queue;
                queuedJob.AddedAt = DateTime.UtcNow;

                connection.Add(queuedJob);

                _notifyNewItemInQueue = true;
            });
        }

        public void AddToSet(string key, string value)
        {
            AddToSet(key, value, 0.0);
        }

        public void AddToSet(string key, string value, double score)
        {
            QueueCommand(connection =>
            {
                var set = connection.Get<ISet>().SingleOrDefault(s => s.Key == key && s.Value == value);
                if (set == null)
                {
                    set = connection.CreateNew<ISet>();

                    set.Id = Guid.NewGuid();
                    set.Key = key;
                    set.Value = value;
                }

                set.Score = (long)score;

                connection.AddOrUpdate(set);
            });
        }

        public void ExpireJob(string jobId, TimeSpan expireIn)
        {
            QueueCommand(connection =>
            {
                var job = connection.Get<IJob>().SingleOrDefault(j => j.Id == Guid.Parse(jobId));
                if (job != null)
                {
                    job.ExpireAt = DateTime.UtcNow.Add(expireIn);

                    connection.Update(job);
                }
            });
        }

        public void IncrementCounter(string key)
        {
            QueueCommand(connection =>
            {
                var counter = connection.Get<ICounter>().Where(c => c.Key == key).OrderByDescending(c => c.Value).FirstOrDefault();
                if (counter != null)
                {
                    counter.Value = counter.Value + 1;
                }
                else
                {
                    counter = connection.CreateNew<ICounter>();

                    counter.Id = Guid.NewGuid();
                    counter.Key = key;
                    counter.Value = 1;
                }

                connection.AddOrUpdate(counter);
            });
        }

        public void IncrementCounter(string key, TimeSpan expireIn)
        {
            QueueCommand(connection =>
            {
                var counter = connection.Get<ICounter>().Where(c => c.Key == key).OrderByDescending(c => c.Value).FirstOrDefault();
                if (counter != null)
                {
                    counter.Value = counter.Value + 1;
                }
                else
                {
                    counter = connection.CreateNew<ICounter>();

                    counter.Id = Guid.NewGuid();
                    counter.Key = key;
                    counter.Value = 1;
                }

                counter.ExpireAt = DateTime.UtcNow.Add(expireIn);

                connection.AddOrUpdate(counter);
            });
        }

        public void DecrementCounter(string key)
        {
            QueueCommand(connection =>
            {
                var counter = connection.Get<ICounter>().Where(c => c.Key == key).OrderByDescending(c => c.Value).FirstOrDefault();
                if (counter != null)
                {
                    counter.Value = counter.Value - 1;
                }
                else
                {
                    counter = connection.CreateNew<ICounter>();

                    counter.Id = Guid.NewGuid();
                    counter.Key = key;
                    counter.Value = 0;
                }

                connection.AddOrUpdate(counter);
            });
        }

        public void DecrementCounter(string key, TimeSpan expireIn)
        {
            QueueCommand(connection =>
            {
                var counter = connection.Get<ICounter>().Where(c => c.Key == key).OrderByDescending(c => c.Value).FirstOrDefault();
                if (counter != null)
                {
                    counter.Value = counter.Value - 1;
                }
                else
                {
                    counter = connection.CreateNew<ICounter>();

                    counter.Id = Guid.NewGuid();
                    counter.Key = key;
                    counter.Value = 0;
                }

                counter.ExpireAt = DateTime.UtcNow.Add(expireIn);

                connection.AddOrUpdate(counter);
            });
        }

        public void InsertToList(string key, string value)
        {
            QueueCommand(connection =>
            {
                var list = connection.CreateNew<IList>();

                list.Id = Guid.NewGuid();
                list.Key = key;
                list.Value = value;

                connection.Add(list);
            });
        }

        public void PersistJob(string jobId)
        {
            QueueCommand(connection =>
            {
                var job = connection.Get<IJob>().SingleOrDefault(j => j.Id == Guid.Parse(jobId));
                if (job != null)
                {
                    job.ExpireAt = null;

                    connection.Update(job);
                }
            });
        }

        public void RemoveFromList(string key, string value)
        {
            QueueCommand(connection =>
            {
                var list = connection.Get<IList>().SingleOrDefault(j => j.Key == key && j.Value == value);
                if (list != null)
                {
                    connection.Delete(list);
                }
            });
        }

        public void RemoveFromSet(string key, string value)
        {
            QueueCommand(connection =>
            {
                var set = connection.Get<ISet>().SingleOrDefault(j => j.Key == key && j.Value == value);
                if (set != null)
                {
                    connection.Delete(set);
                }
            });
        }

        public void RemoveHash(string key)
        {
            Verify.ArgumentNotNull(key, "key");

            QueueCommand(connection =>
            {
                var hash = connection.Get<IHash>().Where(j => j.Key == key);
                if (hash.Any())
                {
                    connection.Delete<IHash>(hash);
                }
            });
        }

        public void SetJobState(string jobId, IHangfireState state)
        {
            QueueCommand(connection =>
            {
                var stateData = connection.CreateNew<IState>();

                stateData.Id = Guid.NewGuid();
                stateData.JobId = Guid.Parse(jobId);
                stateData.Name = state.Name;
                stateData.Reason = state.Reason;
                stateData.CreatedAt = DateTime.UtcNow;
                stateData.Data = JobHelper.ToJson(state.SerializeData());

                connection.Add(stateData);

                var job = connection.Get<IJob>().SingleOrDefault(j => j.Id == Guid.Parse(jobId));
                if (job != null)
                {
                    job.StateId = stateData.Id;
                    job.StateName = state.Name;

                    connection.Update(job);
                }
            });
        }

        public void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            Verify.ArgumentNotNull(key, nameof(key));
            Verify.ArgumentNotNull(keyValuePairs, nameof(keyValuePairs));

            using (_connection.AcquireDistributedLock("locks:SetRangeInHash", TimeSpan.FromMinutes(1)))
            {
                foreach (var kvp in keyValuePairs)
                {
                    var local = kvp;

                    QueueCommand(connection =>
                    {
                        var hash = connection.Get<IHash>().SingleOrDefault(h => h.Key == key && h.Field == local.Key);
                        if (hash == null)
                        {
                            hash = connection.CreateNew<IHash>();

                            hash.Id = Guid.NewGuid();
                            hash.Key = key;
                            hash.Field = local.Key;
                        }

                        hash.Value = local.Value;

                        connection.AddOrUpdate(hash);
                    });
                }
            }
        }

        public void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {

        }

        public void Commit()
        {
            using (var transaction = new TransactionScope())
            {
                foreach (var cmd in _queue)
                {
                    cmd(_connection);
                }

                _queue.Clear();
                transaction.Complete();
            }

            if (_notifyNewItemInQueue)
            {
                _connection.NotifyNewItemInQueue();

                _notifyNewItemInQueue = false;
            }
        }

        private void QueueCommand(Action<CompositeC1Connection> command)
        {
            _queue.Add(command);
        }

        public void Dispose() { }
    }
}
