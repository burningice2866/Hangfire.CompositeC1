using Composite.Data;

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
    }
}
