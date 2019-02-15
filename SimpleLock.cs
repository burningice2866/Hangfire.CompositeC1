using System;
using System.Collections.Concurrent;
using System.Threading;

using Composite;

namespace Hangfire.CompositeC1
{
    public class SimpleLock : IDisposable
    {
        private static readonly ConcurrentDictionary<string, object> Locks = new ConcurrentDictionary<string, object>();

        private readonly object _lock;

        private SimpleLock(string resource, TimeSpan timeout)
        {
            Verify.ArgumentNotNullOrEmpty(resource, "resource");

            if (timeout.TotalSeconds > int.MaxValue)
            {
                throw new ArgumentException($"The timeout specified is too large. Please supply a timeout equal to or less than {int.MaxValue} seconds", nameof(timeout));
            }

            if (timeout.TotalMilliseconds > int.MaxValue)
            {
                throw new ArgumentException($"The timeout specified is too large. Please supply a timeout equal to or less than {(int)TimeSpan.FromMilliseconds(int.MaxValue).TotalSeconds} seconds", nameof(timeout));
            }

            _lock = Locks.GetOrAdd(resource, s => new object());

            Monitor.TryEnter(_lock, timeout);
        }

        public static IDisposable AcquireLock(string resource, TimeSpan timeout)
        {
            return new SimpleLock(resource, timeout);
        }

        public void Dispose()
        {
            Monitor.Exit(_lock);
        }
    }
}
