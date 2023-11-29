using System;
using Cassandra.Observers.Abstractions;
using Cassandra.Requests;

namespace Cassandra.OpenTelemetry
{
    internal class TracesRequestObserver : IRequestObserver
    {
        private readonly IRequestTracker _requestTracker;

        public TracesRequestObserver(IRequestTracker requestTracker)
        {
            _requestTracker = requestTracker;
        }
        public void OnNodeRequestError(Host host, RequestErrorType errorType, RetryDecision.RetryDecisionType decision)
        {
        }

        public void OnRequestSuccess(RequestTrackingInfo r)
        {
            _requestTracker.OnSuccessAsync(r);
        }

        public void OnRequestFailure(Exception ex, RequestTrackingInfo r)
        {
            _requestTracker.OnErrorAsync(r, ex);
        }

        public void OnRequestStart(RequestTrackingInfo r)
        {
            _requestTracker.OnStartAsync(r);
        }

        public void OnSpeculativeExecution(Host host, long delay)
        {
        }
    }
}
