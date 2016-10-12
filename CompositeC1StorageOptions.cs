using System;

using Composite;

namespace Hangfire.CompositeC1
{
    public class CompositeC1StorageOptions
    {
        public TimeSpan JobExpirationCheckInterval { get; set; }
        public TimeSpan CountersAggregateInterval { get; set; }

        private TimeSpan _queuePollInterval;
        public TimeSpan QueuePollInterval
        {
            get { return _queuePollInterval; }

            set
            {
                var message = String.Format("The QueuePollInterval property value should be positive. Given: {0}.", value);

                Verify.IsTrue(value != TimeSpan.Zero, message);
                Verify.IsTrue(value == value.Duration(), message);

                _queuePollInterval = value;
            }
        }

        public int? DashboardJobListLimit { get; set; }

        public CompositeC1StorageOptions()
        {
            JobExpirationCheckInterval = TimeSpan.FromHours(1);
            CountersAggregateInterval = TimeSpan.FromMinutes(5);
            QueuePollInterval = TimeSpan.FromSeconds(15);
            DashboardJobListLimit = 10000;
        }
    }
}
