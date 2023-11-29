using Cassandra.Metrics.Internal;
using Cassandra.Observers;
using Cassandra.Observers.Abstractions;

namespace Cassandra.OpenTelemetry
{
    internal class TracerObserverFactoryBuilder : IObserverFactoryBuilder
    {
        private readonly IRequestTracker tracer;

        public TracerObserverFactoryBuilder(IRequestTracker tracer)
        {
            this.tracer = tracer;
        }

        public IObserverFactory Build(IMetricsManager manager)
        {
            return this.tracer != null ? new TracerObserverFactory(this.tracer) : NullObserverFactory.Instance;
        }
    }
}
