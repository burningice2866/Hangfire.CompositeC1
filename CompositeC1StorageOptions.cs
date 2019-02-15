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
            get => _queuePollInterval;

            set
            {
                var message = $"The QueuePollInterval property value should be positive. Given: {value}.";

                Verify.IsTrue(value != TimeSpan.Zero, message);
                Verify.IsTrue(value == value.Duration(), message);

                _queuePollInterval = value;
            }
        }

        private TimeSpan _slidingInvisibilityTimeout;
        public TimeSpan SlidingInvisibilityTimeout
        {
            get => _slidingInvisibilityTimeout;
            set
            {
                if (value <= TimeSpan.Zero)
                {
                    throw new ArgumentOutOfRangeException("Sliding timeout should be greater than zero");
                }

                _slidingInvisibilityTimeout = value;
            }
        }

        public int? DashboardJobListLimit { get; set; }

        public CompositeC1StorageOptions()
        {
            JobExpirationCheckInterval = TimeSpan.FromHours(1);
            CountersAggregateInterval = TimeSpan.FromMinutes(5);
            QueuePollInterval = TimeSpan.FromSeconds(15);
            SlidingInvisibilityTimeout = TimeSpan.FromSeconds(10);
            DashboardJobListLimit = 10000;
        }
    }
}
