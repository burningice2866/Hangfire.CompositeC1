using System;
using System.Linq;
using System.Threading;

using Composite;

using Hangfire.Annotations;
using Hangfire.CompositeC1.Types;
using Hangfire.Logging;
using Hangfire.Storage;

namespace Hangfire.CompositeC1
{
    public class QueuedJob : IFetchedJob
    {
        private static readonly object SyncRoot = new object();

        private readonly ILog _logger = LogProvider.GetCurrentClassLogger();
        
        private readonly CompositeC1Storage _storage;
        private readonly Timer _timer;
        private bool _disposed;
        private bool _removedFromQueue;
        private bool _requeued;

        private readonly Guid _id;
        private readonly DateTime? _fetchedAt;
        private readonly string _queue;

        public string JobId { get; }

        public QueuedJob([NotNull] CompositeC1Storage storage,
            Guid id,
            [NotNull] string jobId,
            [NotNull] string queue,
            [NotNull] DateTime? fetchedAt)
        {
            Verify.ArgumentNotNull(storage, nameof(storage));
            Verify.ArgumentNotNull(jobId, nameof(jobId));
            Verify.ArgumentNotNull(queue, nameof(queue));
            Verify.ArgumentNotNull(fetchedAt, nameof(fetchedAt));

            _storage = storage;
            _id = id;
            _queue = queue;
            _fetchedAt = fetchedAt;

            JobId = jobId;

            var keepAliveInterval = TimeSpan.FromSeconds(storage.Options.SlidingInvisibilityTimeout.TotalSeconds / 5);
            _timer = new Timer(ExecuteKeepAliveQuery, null, keepAliveInterval, keepAliveInterval);
        }

        public void RemoveFromQueue()
        {
            lock (SyncRoot)
            {
                if (!_fetchedAt.HasValue)
                {
                    return;
                }

                _storage.UseConnection(connection =>
                {
                    var queuedJob = connection.Get<IJobQueue>().SingleOrDefault(q => q.Id == _id && q.Queue == _queue && q.FetchedAt == _fetchedAt);
                    if (queuedJob != null)
                    {
                        connection.Delete(queuedJob);
                    }
                });

                _removedFromQueue = true;
            }
        }

        public void Requeue()
        {
            lock (SyncRoot)
            {
                if (!_fetchedAt.HasValue)
                {
                    return;
                }

                _storage.UseConnection(connection =>
                {
                    var queuedJob = connection.Get<IJobQueue>().SingleOrDefault(q => q.Id == _id && q.FetchedAt == _fetchedAt);
                    if (queuedJob != null)
                    {
                        queuedJob.FetchedAt = null;

                        connection.Update(queuedJob);
                    }
                });

                _requeued = true;
            }
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;

            _timer?.Dispose();

            lock (SyncRoot)
            {
                if (!_removedFromQueue && !_requeued)
                {
                    Requeue();
                }
            }
        }

        private void ExecuteKeepAliveQuery(object obj)
        {
            lock (SyncRoot)
            {
                if (_requeued || _removedFromQueue)
                {
                    return;
                }

                try
                {
                    _storage.UseConnection(connection =>
                    {
                        var queuedJob = connection.Get<IJobQueue>().SingleOrDefault(q => q.Id == _id);
                        if (queuedJob != null)
                        {
                            queuedJob.FetchedAt = DateTime.UtcNow;

                            connection.Update(queuedJob);
                        }
                    });

                    _logger.Trace($"Keep-alive query for message {_id} sent");
                }
                catch (Exception ex)
                {
                    _logger.DebugException($"Unable to execute keep-alive query for message {_id}", ex);
                }
            }
        }
    }
}
