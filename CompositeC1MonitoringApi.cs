using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

using Composite.Data;

using Hangfire.Common;
using Hangfire.CompositeC1.Entities;
using Hangfire.CompositeC1.Types;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;

using IState = Hangfire.CompositeC1.Types.IState;

namespace Hangfire.CompositeC1
{
    public class CompositeC1MonitoringApi : IMonitoringApi
    {
        public JobList<DeletedJobDto> DeletedJobs(int from, int count)
        {
            return GetJobs(
                from,
                count,
                DeletedState.StateName,
                (jsonJob, job, stateData) => new DeletedJobDto
                {
                    Job = job,
                    DeletedAt = JobHelper.DeserializeNullableDateTime(stateData["DeletedAt"])
                });
        }

        public long DeletedListCount()
        {
            return GetNumberOfJobsByStateName(DeletedState.StateName);
        }

        public long EnqueuedCount(string queue)
        {
            using (var data = new DataConnection())
            {
                return data.Get<IJobQueue>().Count(q => q.Queue == queue && !q.FetchedAt.HasValue);
            }
        }

        public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int from, int perPage)
        {
            var enqueuedJobIds = QueueApi.GetEnqueuedJobIds(queue, from, perPage);

            return EnqueuedJobs(enqueuedJobIds);
        }

        public IDictionary<DateTime, long> FailedByDatesCount()
        {
            return GetTimelineStats("failed");
        }

        public long FailedCount()
        {
            return GetNumberOfJobsByStateName(FailedState.StateName);
        }

        public JobList<FailedJobDto> FailedJobs(int from, int count)
        {
            return GetJobs(from, count,
                FailedState.StateName,
                (jsonJob, job, stateData) => new FailedJobDto
                {
                    Job = job,
                    Reason = jsonJob.StateReason,
                    ExceptionDetails = stateData["ExceptionDetails"],
                    ExceptionMessage = stateData["ExceptionMessage"],
                    ExceptionType = stateData["ExceptionType"],
                    FailedAt = JobHelper.DeserializeNullableDateTime(stateData["FailedAt"])
                });
        }

        public long FetchedCount(string queue)
        {
            using (var data = new DataConnection())
            {
                return data.Get<IJobQueue>().Count(q => q.Queue == queue && q.FetchedAt.HasValue);
            }
        }

        public JobList<FetchedJobDto> FetchedJobs(string queue, int from, int perPage)
        {
            var fetchedJobIds = QueueApi.GetFetechedJobIds(queue, from, perPage);

            return FetchedJobs(fetchedJobIds);
        }

        public StatisticsDto GetStatistics()
        {
            using (var data = new DataConnection())
            {
                var states =
                    data.Get<IJob>()
                        .Where(j => j.StateName != null)
                        .GroupBy(j => j.StateName)
                        .ToDictionary(j => j.Key, j => j.Count());

                Func<string, int> getCountIfExists = name => states.ContainsKey(name) ? states[name] : 0;

                var succeded = data.Get<ICounter>().Where(c => c.Key == "stats:succeeded").Sum(c => (int?)c.Value);
                var deleted = data.Get<ICounter>().Where(c => c.Key == "stats:deleted").Sum(c => (int?)c.Value);
                var recurringJobs = data.Get<ISet>().Count(c => c.Key == "recurring-jobs");
                var servers = data.Get<IServer>().Count();

                var stats = new StatisticsDto
                {
                    Enqueued = getCountIfExists(EnqueuedState.StateName),
                    Failed = getCountIfExists(FailedState.StateName),
                    Processing = getCountIfExists(ProcessingState.StateName),
                    Scheduled = getCountIfExists(ScheduledState.StateName),

                    Servers = servers,

                    Succeeded = succeded.HasValue ? succeded.Value : 0,
                    Deleted = deleted.HasValue ? deleted.Value : 0,
                    Recurring = recurringJobs
                };

                return stats;
            }
        }

        public IDictionary<DateTime, long> HourlyFailedJobs()
        {
            return GetHourlyTimelineStats("failed");
        }

        public IDictionary<DateTime, long> HourlySucceededJobs()
        {
            return GetHourlyTimelineStats("succeeded");
        }

        public JobDetailsDto JobDetails(string jobId)
        {
            using (var data = new DataConnection())
            {
                var g = Guid.Parse(jobId);

                var job = data.Get<IJob>().SingleOrDefault(j => j.Id == g);
                if (job == null)
                {
                    return null;
                }

                var jobParameters = data.Get<IJobParameter>().Where(p => p.Id == g).ToDictionary(p => p.Name, p => p.Value);

                var state = data.Get<IState>().Where(s => s.Id == g).ToList().Select(x => new StateHistoryDto
                {
                    StateName = x.Name,
                    CreatedAt = x.CreatedAt,
                    Reason = x.Reason,
                    Data = JobHelper.FromJson<Dictionary<string, string>>(x.Data)
                }).ToList();

                return new JobDetailsDto
                {
                    CreatedAt = job.CreatedAt,
                    Job = DeserializeJob(job.InvocationData, job.Arguments),
                    History = state,
                    Properties = jobParameters
                };
            }
        }

        public long ProcessingCount()
        {
            return GetNumberOfJobsByStateName(ProcessingState.StateName);
        }

        public JobList<ProcessingJobDto> ProcessingJobs(int from, int count)
        {
            return GetJobs(from, count,
                ProcessingState.StateName,
                (jsonJob, job, stateData) => new ProcessingJobDto
                {
                    Job = job,
                    ServerId = stateData.ContainsKey("ServerId") ? stateData["ServerId"] : stateData["ServerName"],
                    StartedAt = JobHelper.DeserializeDateTime(stateData["StartedAt"]),
                });
        }

        public IList<QueueWithTopEnqueuedJobsDto> Queues()
        {
            using (var data = new DataConnection())
            {
                var queuesData = data.Get<IJobQueue>().ToList();
                var queues = queuesData.GroupBy(q => q.Queue).ToDictionary(q => q.Key, q => q.Count());
                var result = new List<QueueWithTopEnqueuedJobsDto>(queues.Keys.Count);

                foreach (var kvp in queues)
                {
                    var enqueuedJobIds = QueueApi.GetEnqueuedJobIds(kvp.Key, 0, 5);
                    var counters = QueueApi.GetEnqueuedAndFetchedCount(kvp.Key);

                    result.Add(new QueueWithTopEnqueuedJobsDto
                    {
                        Name = kvp.Key,
                        Length = counters.EnqueuedCount,
                        Fetched = counters.FetchedCount,
                        FirstJobs = EnqueuedJobs(enqueuedJobIds)
                    });
                }

                return result;
            }
        }

        public long ScheduledCount()
        {
            return GetNumberOfJobsByStateName(ScheduledState.StateName);
        }

        public JobList<ScheduledJobDto> ScheduledJobs(int from, int count)
        {
            return GetJobs(from, count,
                ScheduledState.StateName,
                (jsonJob, job, stateData) => new ScheduledJobDto
                {
                    Job = job,
                    EnqueueAt = JobHelper.DeserializeDateTime(stateData["EnqueueAt"]),
                    ScheduledAt = JobHelper.DeserializeDateTime(stateData["ScheduledAt"])
                });
        }

        public IList<ServerDto> Servers()
        {
            var result = new List<ServerDto>();

            using (var data = new DataConnection())
            {
                foreach (var server in data.Get<IServer>())
                {
                    var serverData = JobHelper.FromJson<ServerData>(server.Data);

                    result.Add(new ServerDto
                    {
                        Name = server.Id,
                        Heartbeat = server.LastHeartbeat,
                        Queues = serverData.Queues,
                        StartedAt = serverData.StartedAt.HasValue ? serverData.StartedAt.Value : DateTime.MinValue,
                        WorkersCount = serverData.WorkerCount
                    });
                }
            }

            return result;
        }

        public IDictionary<DateTime, long> SucceededByDatesCount()
        {
            return GetTimelineStats("succeeded");
        }

        public JobList<SucceededJobDto> SucceededJobs(int from, int count)
        {
            return GetJobs(from, count,
                SucceededState.StateName,
                (jsonJob, job, stateData) => new SucceededJobDto
                {
                    Job = job,
                    TotalDuration = stateData.ContainsKey("PerformanceDuration") && stateData.ContainsKey("Latency")
                        ? (long?)long.Parse(stateData["PerformanceDuration"]) + (long?)long.Parse(stateData["Latency"])
                        : null,
                    SucceededAt = JobHelper.DeserializeNullableDateTime(stateData["SucceededAt"])
                });
        }

        public long SucceededListCount()
        {
            return GetNumberOfJobsByStateName(SucceededState.StateName);
        }

        private static Dictionary<DateTime, long> GetHourlyTimelineStats(string type)
        {
            var endDate = DateTime.UtcNow;
            var dates = new List<DateTime>();

            for (var i = 0; i < 24; i++)
            {
                dates.Add(endDate);
                endDate = endDate.AddHours(-1);
            }

            var keys = dates.Select(x => String.Format("stats:{0}:{1}", type, x.ToString("yyyy-MM-dd-HH"))).ToList();

            return MapKeysToDateTimeLongs(dates, keys);
        }

        public Dictionary<DateTime, long> GetTimelineStats(string type)
        {
            var endDate = DateTime.UtcNow.Date;
            var startDate = endDate.AddDays(-7);
            var dates = new List<DateTime>();

            while (startDate <= endDate)
            {
                dates.Add(endDate);
                endDate = endDate.AddDays(-1);
            }

            var stringDates = dates.Select(x => x.ToString("yyyy-MM-dd")).ToList();
            var keys = stringDates.Select(x => String.Format("stats:{0}:{1}", type, x)).ToList();

            return MapKeysToDateTimeLongs(dates, keys);
        }

        private static Dictionary<DateTime, long> MapKeysToDateTimeLongs(List<DateTime> dates, List<string> keys)
        {
            IDictionary<string, int> valuesMap;

            using (var data = new DataConnection())
            {
                valuesMap = data.Get<ICounter>()
                    .Where(c => keys.Contains(c.Key))
                    .GroupBy(c => c.Key)
                    .ToDictionary(o => o.Key, o => o.First().Value);
            }

            foreach (var key in keys.Where(key => !valuesMap.ContainsKey(key)))
            {
                valuesMap.Add(key, 0);
            }

            var result = new Dictionary<DateTime, long>();
            for (var i = 0; i < dates.Count; i++)
            {
                var value = valuesMap[valuesMap.Keys.ElementAt(i)];

                result.Add(dates[i], value);
            }

            return result;
        }

        private static JobList<FetchedJobDto> FetchedJobs(IEnumerable<Guid> jobIds)
        {
            using (var data = new DataConnection())
            {
                var jobs = data.Get<IJob>();
                var states = data.Get<IState>();
                var queues = data.Get<IJobQueue>();

                var query = (from j in jobs
                             join s in states on j.StateId equals s.Id
                             join q in queues on j.Id equals q.JobId
                             where jobIds.Contains(j.Id) && q.FetchedAt.HasValue
                             select new JsonJob
                             {
                                 Arguments = j.Arguments,
                                 CreatedAt = j.CreatedAt,
                                 ExpireAt = j.ExpireAt,
                                 Id = j.Id.ToString(),
                                 InvocationData = j.InvocationData,
                                 StateReason = s.Reason,
                                 StateData = s.Data,
                                 StateName = j.StateName
                             }).ToList();

                var result = new List<KeyValuePair<string, FetchedJobDto>>(query.Count);

                foreach (var job in query)
                {
                    result.Add(new KeyValuePair<string, FetchedJobDto>(
                        job.Id,
                        new FetchedJobDto
                        {
                            Job = DeserializeJob(job.InvocationData, job.Arguments),
                            State = job.StateName,
                            FetchedAt = job.FetchedAt
                        }));
                }

                return new JobList<FetchedJobDto>(result);
            }
        }

        private static JobList<EnqueuedJobDto> EnqueuedJobs(IEnumerable<Guid> jobIds)
        {
            using (var data = new DataConnection())
            {
                var jobs = data.Get<IJob>();
                var states = data.Get<IState>();
                var queues = data.Get<IJobQueue>();

                var query = (from j in jobs
                             join s in states on j.StateId equals s.Id
                             join q in queues on j.Id equals q.JobId
                             where jobIds.Contains(j.Id) && !q.FetchedAt.HasValue
                             select new JsonJob
                             {
                                 Arguments = j.Arguments,
                                 CreatedAt = j.CreatedAt,
                                 ExpireAt = j.ExpireAt,
                                 Id = j.Id.ToString(),
                                 InvocationData = j.InvocationData,
                                 StateReason = s.Reason,
                                 StateData = s.Data,
                                 StateName = j.StateName
                             }).ToList();

                return DeserializeJobs(query, (jsonJob, job, stateData) => new EnqueuedJobDto
                {
                    Job = job,
                    State = jsonJob.StateName,
                    EnqueuedAt = jsonJob.StateName == EnqueuedState.StateName
                        ? JobHelper.DeserializeNullableDateTime(stateData["EnqueuedAt"])
                        : null
                });
            }
        }

        private static JobList<TDto> GetJobs<TDto>(
            int from,
            int count,
            string stateName,
            Func<JsonJob, Job, Dictionary<string, string>, TDto> selector)
        {
            using (var data = new DataConnection())
            {
                var jobs = data.Get<IJob>().Where(j => j.StateName == stateName).OrderByDescending(j => j.CreatedAt);
                var states = data.Get<IState>();

                var query = (from job in jobs
                             join state in states on job.StateId equals state.Id
                             select new JsonJob
                             {
                                 Id = job.Id.ToString(),
                                 InvocationData = job.InvocationData,
                                 Arguments = job.Arguments,
                                 CreatedAt = job.CreatedAt,
                                 ExpireAt = job.ExpireAt,
                                 StateReason = state.Reason,
                                 StateData = state.Data
                             }
                    ).Skip(from).Take(count).ToList();

                return DeserializeJobs(query, selector);
            }
        }

        private static Job DeserializeJob(string invocationData, string arguments)
        {
            var data = JobHelper.FromJson<InvocationData>(invocationData);
            data.Arguments = arguments;

            try
            {
                return data.Deserialize();
            }
            catch (JobLoadException)
            {
                return null;
            }
        }

        private static JobList<TDto> DeserializeJobs<TDto>(
            IList<JsonJob> jobs,
            Func<JsonJob, Job, Dictionary<string, string>, TDto> selector)
        {
            var result = new List<KeyValuePair<string, TDto>>(jobs.Count);

            foreach (var job in jobs)
            {
                var stateData = JobHelper.FromJson<Dictionary<string, string>>(job.StateData);
                var dto = selector(job, DeserializeJob(job.InvocationData, job.Arguments), stateData);

                result.Add(new KeyValuePair<string, TDto>(
                    job.Id.ToString(CultureInfo.InvariantCulture), dto));
            }

            return new JobList<TDto>(result);
        }

        private static long GetNumberOfJobsByStateName(string stateName)
        {
            using (var data = new DataConnection())
            {
                var count = data.Get<IJob>().Count(j => j.StateName == stateName);

                return count;
            }
        }
    }
}
