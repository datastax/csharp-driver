using System.Linq;
using Cassandra.Metrics.Internal;
using Cassandra.Observers.Abstractions;

namespace Cassandra.Observers
{
    internal class CompositeObserverFactoryBuilder : IObserverFactoryBuilder
    {
        private readonly IObserverFactoryBuilder[] builders;

        public CompositeObserverFactoryBuilder(params IObserverFactoryBuilder[] builders)
        {
            this.builders = builders;
        }

        public IObserverFactory Build(IMetricsManager manager)
        {
            return new CompositeObserverFactory(this.builders
                .Select(b => b.Build(manager))
                .ToArray());
        }
    }
}
