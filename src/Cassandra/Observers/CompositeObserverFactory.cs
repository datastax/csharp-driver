using System.Collections.Generic;
using System.Linq;
using Cassandra.Observers.Abstractions;

namespace Cassandra.Observers
{
    internal class CompositeObserverFactory : IObserverFactory
    {
        private readonly IEnumerable<IObserverFactory> factories;

        public CompositeObserverFactory(IEnumerable<IObserverFactory> factories)
        {
            this.factories = factories;
        }
        public IConnectionObserver CreateConnectionObserver(Host host)
        {
            return NullConnectionObserver.Instance;
        }

        public IRequestObserver CreateRequestObserver()
        {
            return new CompositeRequestObserver(
                this.factories.Select(x => x.CreateRequestObserver()));
        }
    }
}
