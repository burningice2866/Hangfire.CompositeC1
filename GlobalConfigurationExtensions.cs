using Hangfire.CompositeC1;

namespace Hangfire
{
    public static class GlobalConfigurationExtensions
    {
        public static IGlobalConfiguration<CompositeC1Storage> UseCompositeC1Storage(this IGlobalConfiguration configuration)
        {
            var storageOptions = new CompositeC1StorageOptions();

            return configuration.UseCompositeC1Storage(storageOptions);
        }

        public static IGlobalConfiguration<CompositeC1Storage> UseCompositeC1Storage(this IGlobalConfiguration configuration, CompositeC1StorageOptions storageOptions)
        {
            var storage = new CompositeC1Storage(storageOptions);

            return configuration.UseStorage(storage);
        }
    }
}
