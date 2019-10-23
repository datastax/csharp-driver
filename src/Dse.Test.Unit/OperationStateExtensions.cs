using System;
using Dse.Metrics;
using Dse.Metrics.Providers.Null;
using Dse.Metrics.Registries;
using Dse.Observers;
using Dse.Responses;

namespace Dse.Test.Unit
{
    internal static class OperationStateExtensions
    {
        public static OperationState CreateMock(Action<Exception, Response> action)
        {
            return new OperationState(
                (error, response) => action(error?.Exception, response), 
                null, 
                0, 
                new MetricsOperationObserver(new NodeMetrics(new NullDriverMetricsProvider(), new DriverMetricsOptions(), false, "c"), false));
        }
    }
}