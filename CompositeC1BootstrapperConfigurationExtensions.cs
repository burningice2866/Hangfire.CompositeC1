namespace Hangfire.CompositeC1
{
    public static class CompositeC1BootstrapperConfigurationExtensions
    {
        public static CompositeC1Storage UseCompositeC1Storage(this IBootstrapperConfiguration configuration)
        {
            var storage = new CompositeC1Storage();

            configuration.UseStorage(storage);

            return storage;
        }
    }
}
