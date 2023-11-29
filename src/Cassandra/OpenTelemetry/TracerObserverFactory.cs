using Cassandra.Observers;
using Cassandra.Observers.Abstractions;

namespace Cassandra.OpenTelemetry
{
    internal class TracerObserverFactory : IObserverFactory
    {
        private readonly IRequestTracker tracer;

        public TracerObserverFactory(IRequestTracker tracer)
        {
            this.tracer = tracer;
        }

        public IConnectionObserver CreateConnectionObserver(Host host)
        {
            return NullConnectionObserver.Instance;
        }

        public IRequestObserver CreateRequestObserver()
        {
            return new TracesRequestObserver(tracer);
        }
    }
}
