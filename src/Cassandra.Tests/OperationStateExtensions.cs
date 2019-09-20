using System;
using Cassandra.Metrics.Providers.Null;
using Cassandra.Metrics.Registries;
using Cassandra.Observers;
using Cassandra.Responses;

namespace Cassandra.Tests
{
    internal static class OperationStateExtensions
    {
        public static OperationState CreateMock(Action<Exception, Response> action)
        {
            return new OperationState(action, null, 0, new OperationObserver(new NodeMetricsRegistry(NullDriverMetricsProvider.Instance)));
        }
    }
}