using System.Linq;

using Composite.Data;

using Hangfire.CompositeC1.Types;

namespace Hangfire.CompositeC1
{
    public static class DataConnectionExtensions
    {
        public static void AddOrUpdate<T>(this DataConnection data, bool add, T obj) where T : class, IData
        {
            if (add)
            {
                data.Add(obj);
            }
            else
            {
                data.Update(obj);
            }
        }

        public static long? GetCombinedCounter(this DataConnection data, string key)
        {
            var counters = data.Get<ICounter>().Where(c => c.Key == key).Select(c => (long?)c.Value);
            var aggregatedCounters = data.Get<IAggregatedCounter>().Where(c => c.Key == key).Select(c => (long?)c.Value);

            return counters.Concat(aggregatedCounters).Sum(v => v);
        }
    }
}
