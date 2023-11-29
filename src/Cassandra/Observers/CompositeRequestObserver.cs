using System;
using System.Collections.Generic;
using Cassandra.Observers.Abstractions;
using Cassandra.Requests;

namespace Cassandra.Observers
{
    internal class CompositeRequestObserver : IRequestObserver
    {
        private readonly IEnumerable<IRequestObserver> observers;

        public CompositeRequestObserver(IEnumerable<IRequestObserver> observers)
        {
            this.observers = observers;
        }

        public void OnNodeRequestError(Host host, RequestErrorType errorType, RetryDecision.RetryDecisionType decision)
        {
            foreach(var observer in this.observers)
            {
                observer.OnNodeRequestError(host, errorType, decision);
            }
        }

        public void OnRequestFailure(Exception ex, RequestTrackingInfo r)
        {
            foreach (var observer in this.observers)
            {
                observer.OnRequestFailure(ex, r);
            }
        }

        public void OnRequestSuccess(RequestTrackingInfo r)
        {
            foreach (var observer in this.observers)
            {
                observer.OnRequestSuccess(r);
            }
        }

        public void OnRequestStart(RequestTrackingInfo requestTrackingInfo)
        {
            foreach (var observer in this.observers)
            {
                observer.OnRequestStart(requestTrackingInfo);
            }
        }

        public void OnSpeculativeExecution(Host host, long delay)
        {
            foreach (var observer in this.observers)
            {
                observer.OnSpeculativeExecution(host, delay);
            }
        }
    }
}
