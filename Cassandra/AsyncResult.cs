using System;
using System.Threading;
using System.Reflection;

//based on http://blogs.msdn.com/b/nikos/archive/2011/03/14/how-to-implement-iasyncresult-in-another-way.aspx

namespace Cassandra
{
    internal class AsyncTimeoutException : TimeoutException
    {
        public AsyncTimeoutException()
            : base("async operation timeout exception")
        {
        }
    }

    internal partial class AsyncResultNoResult : IAsyncResult
    {
        // Fields set at construction which never change while 
        // operation is pending
        private readonly AsyncCallback _asyncCallback;
        private readonly object _asyncState;

        // Fields set at construction which do change after 
        // operation completes
        private const int StatePending = 0;
        private const int StateCompletedSynchronously = 1;
        private const int StateCompletedAsynchronously = 2;
        private int _completedState = StatePending;

        // Field that may or may not get set depending on usage
        private MyWaitHandle _asyncWaitHandle;

        // Fields set when operation completes
        private Exception _exception;

        /// <summary>
        /// The object which started the operation.
        /// </summary>
        private readonly object _owner;

        /// <summary>
        /// Used to verify the BeginXXX and EndXXX calls match.
        /// </summary>
        private string _operationId;

        /// <summary>
        /// The object which is a source of the operation.
        /// </summary>
        private readonly object _sender;

        /// <summary>
        /// The tag object 
        /// </summary>
        private readonly object _tag;

        private readonly int _timeout;
        private readonly DateTimeOffset _started;

        internal AsyncResultNoResult(
            AsyncCallback asyncCallback,
            object state,
            object owner,
            string operationId,
            object sender,
            object tag,
            int timeout)
        {
            _asyncCallback = asyncCallback;
            _asyncState = state;
            _owner = owner;
            _operationId =
                string.IsNullOrEmpty(operationId) ? string.Empty : operationId;
            _sender = sender;
            _tag = tag;
            _timeout = timeout;
            if (_timeout != Timeout.Infinite)
                _started = DateTimeOffset.Now;
        }

        internal bool Complete()
        {
            return this.Complete(null, false /*completedSynchronously*/);
        }

        internal bool Complete(bool completedSynchronously)
        {
            return this.Complete(null, completedSynchronously);
        }

        internal bool Complete(Exception exception)
        {
            return this.Complete(exception, false /*completedSynchronously*/);
        }

        internal bool Complete(Exception exception, bool completedSynchronously)
        {
            bool result = false;

            // The _completedState field MUST be set prior calling the callback
            int prevState = Interlocked.Exchange(ref _completedState,
                completedSynchronously ? StateCompletedSynchronously :
                StateCompletedAsynchronously);
            if (prevState == StatePending)
            {
                // Passing null for exception means no error occurred. 
                // This is the common case
                _exception = exception;

                // Do any processing before completion.
                this.Completing(exception, completedSynchronously);

                // If the event exists, set it
                if (_asyncWaitHandle != null) _asyncWaitHandle.Set();

                this.MakeCallback(_asyncCallback, this);

                // Do any final processing after completion
                this.Completed(exception, completedSynchronously);

                result = true;
            }
            else
            {
                //already set
            }
            return result;
        }

        private void CheckUsage(object owner, string operationId)
        {
            if (!object.ReferenceEquals(owner, _owner))
            {
                throw new InvalidOperationException(
                    "End was called on a different object than Begin.");
            }

            // Reuse the operation ID to detect multiple calls to end.
            if (object.ReferenceEquals(null, _operationId))
            {
                throw new InvalidOperationException(
                    "End was called multiple times for this operation.");
            }

            if (!string.Equals(operationId, _operationId))
            {
                throw new ArgumentException(
                    "End operation type was different than Begin.");
            }

            // Mark that End was already called.
            _operationId = null;
        }

        public static void End(
            IAsyncResult result, object owner, string operationId)
        {
            var asyncResult = result as AsyncResultNoResult;
            if (asyncResult == null)
            {
                throw new ArgumentException(
                    "Result passed represents an operation not supported " +
                    "by this framework.",
                    "result");
            }

            asyncResult.CheckUsage(owner, string.IsNullOrEmpty(operationId) ? string.Empty : operationId);

            // This method assumes that only 1 thread calls EndInvoke 
            // for this object
            if (!asyncResult.IsCompleted)
            {
                // If the operation isn't done, wait for it
                asyncResult.AsyncWaitHandle.WaitOne(Timeout.Infinite);
                asyncResult.AsyncWaitHandle.Close();
                if (!asyncResult.IsCompleted)
                {
                    asyncResult.Complete(new AsyncTimeoutException());
                }
                asyncResult._asyncWaitHandle = null;  // Allow early GC                
            }
            // Operation is done: if an exception occurred, throw it
            if (asyncResult._exception != null)
            {
                var mth = typeof(Exception).GetMethod("InternalPreserveStackTrace", BindingFlags.Instance | BindingFlags.NonPublic);
                if(mth!=null)
                    mth.Invoke(asyncResult._exception, null);
                throw asyncResult._exception;
            }
        }

        #region Implementation of IAsyncResult

        public object AsyncState { get { return _asyncState; } }
        public object AsyncOwner { get { return _owner; } }
        public object AsyncSender { get { return _sender; } }
        public object Tag { get { return _tag; } }

        public bool CompletedSynchronously
        {
            get
            {
                return Thread.VolatileRead(ref _completedState) ==
                    StateCompletedSynchronously;
            }
        }

        class MyWaitHandle : WaitHandle
        {
            readonly ManualResetEvent _resetEvent;
            readonly DateTimeOffset _started;
            readonly int _timeout;
            public MyWaitHandle(bool state, DateTimeOffset started, int timeout) { _resetEvent = new ManualResetEvent(state); this._started = started; this._timeout = timeout; }
            public void Set() { _resetEvent.Set(); }
            public override bool WaitOne()
            {
                return WaitOne(Timeout.Infinite);
            }
            public override bool WaitOne(TimeSpan timeout)
            {
                return WaitOne((int)timeout.TotalMilliseconds);
            }
            public override bool WaitOne(int millisecondsTimeout)
            {
                if (_timeout == Timeout.Infinite)
                {
                    return _resetEvent.WaitOne(millisecondsTimeout);
                }
                else
                {
                    var fixTim = _timeout - (int)(DateTimeOffset.Now - _started).TotalMilliseconds;
                    if (fixTim < 0) fixTim = 0;

                    var tim = millisecondsTimeout == Timeout.Infinite ?
                        fixTim
                       : Math.Min(millisecondsTimeout, fixTim);
                    if (millisecondsTimeout == Timeout.Infinite || millisecondsTimeout <= fixTim)
                        return _resetEvent.WaitOne(tim);
                    else
                    {
                        _resetEvent.WaitOne(tim);
                        return true;
                    }
                }
            }
        }

        public WaitHandle AsyncWaitHandle
        {
            get
            {
                if (_asyncWaitHandle == null)
                {
                    bool done = IsCompleted;
                    var myWaitHandle = new MyWaitHandle(done, _started, _timeout);
                    if (Interlocked.CompareExchange(ref _asyncWaitHandle,
                        myWaitHandle, null) != null)
                    {
                        // Another thread created this object's event; dispose 
                        // the event we just created
                        myWaitHandle.Close();
                    }
                    else
                    {
                        if (!done && IsCompleted)
                        {
                            // If the operation wasn't done when we created 
                            // the event but now it is done, set the event
                            _asyncWaitHandle.Set();
                        }
                    }
                }
                return _asyncWaitHandle;
            }
        }

        public bool IsCompleted
        {
            get
            {
                return Thread.VolatileRead(ref _completedState) !=
                    StatePending;
            }
        }
        #endregion

        #region Extensibility

        protected virtual void Completing(
            Exception exception, bool completedSynchronously)
        {
        }

        protected virtual void MakeCallback(
            AsyncCallback callback, AsyncResultNoResult result)
        {
            // If a callback method was set, call it
            if (callback != null)
                callback(result);
        }

        protected virtual void Completed(
            Exception exception, bool completedSynchronously)
        {
        }
        #endregion
    }

    internal partial class AsyncResult<TResult> : AsyncResultNoResult
    {
        // Field set when operation completes
        private TResult _result = default(TResult);

        internal void SetResult(TResult result)
        {
            _result = result;
        }

        internal AsyncResult(AsyncCallback asyncCallback, object state, object owner, string operationId, object sender,
                             object tag, int timeout) :
                                 base(asyncCallback, state, owner, operationId, sender, tag, timeout)
        {
        }

        new public static TResult End(IAsyncResult result, object owner, string operationId)
        {
            var asyncResult = result as AsyncResult<TResult>;
            if (asyncResult == null)
            {
                throw new ArgumentException(
                    "Result passed represents an operation not supported " +
                    "by this framework.",
                    "result");
            }

            // Wait until operation has completed 
            AsyncResultNoResult.End(result, owner, operationId);

            // Return the result (if above didn't throw)
            return asyncResult._result;
        }
    }
}
