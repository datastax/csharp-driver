using System;
using System.Reflection;
using System.Threading;

namespace Cassandra
{
    internal class AsyncResultNoResult : IAsyncResult
    {
        // Fields set at construction which do change after 
        // operation completes
        private const int StatePending = 0;
        private const int StateCompletedSynchronously = 1;
        private const int StateCompletedAsynchronously = 2;
        private static long _curId;

        private readonly AsyncCallback _asyncCallback;
        private readonly object _asyncState;

        // Field that may or may not get set depending on usage
        private readonly ManualResetEventSlim _asyncWaitHandle = new ManualResetEventSlim(false);

        // Fields set when operation completes

        /// <summary>
        /// The object which started the operation.
        /// </summary>
        private readonly object _owner;

        /// <summary>
        /// The object which is a source of the operation.
        /// </summary>
        private readonly object _sender;

        /// <summary>
        /// The tag object 
        /// </summary>
        private readonly object _tag;

        public long Id;
        private int _completedState = StatePending;
        private Exception _exception;

        /// <summary>
        /// Used to verify the BeginXXX and EndXXX calls match.
        /// </summary>
        private string _operationId;

        internal AsyncResultNoResult(
            AsyncCallback asyncCallback,
            object state,
            object owner,
            string operationId,
            object sender,
            object tag)
        {
            Id = Interlocked.Increment(ref _curId);
            _asyncCallback = asyncCallback;
            _asyncState = state;
            _owner = owner;
            _operationId =
                string.IsNullOrEmpty(operationId) ? string.Empty : operationId;
            _sender = sender;
            _tag = tag;
        }

        internal bool Complete()
        {
            return Complete(null, false /*completedSynchronously*/);
        }

        internal bool Complete(bool completedSynchronously)
        {
            return Complete(null, completedSynchronously);
        }

        internal bool Complete(Exception exception)
        {
            return Complete(exception, false /*completedSynchronously*/);
        }

        internal bool Complete(Exception exception, bool completedSynchronously)
        {
            // The _completedState field MUST be set prior calling the callback
            if (Interlocked.Exchange(ref _completedState,
                                     completedSynchronously
                                         ? StateCompletedSynchronously
                                         : StateCompletedAsynchronously) == StatePending)
            {
                // Passing null for exception means no error occurred. 
                // This is the common case
                _exception = exception;

                // Do any processing before completion.
                Completing(exception, completedSynchronously);

                _asyncWaitHandle.Set();

                MakeCallback(_asyncCallback, this);

                // Do any final processing after completion
                Completed(exception, completedSynchronously);

                return true;
            }
            return false;
        }

        private void CheckUsage(object owner, string operationId)
        {
            if (!ReferenceEquals(owner, _owner))
            {
                throw new InvalidOperationException(
                    "End was called on a different object than Begin.");
            }

            // Reuse the operation ID to detect multiple calls to end.
            if (ReferenceEquals(null, _operationId))
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

            try
            {
                asyncResult.AsyncWaitHandle.WaitOne(Timeout.Infinite);
            }
            catch (ThreadInterruptedException tiex)
            {
                if (!asyncResult.IsCompleted)
                    asyncResult.Complete(tiex);
            }

            // Operation is done: if an exception occurred, throw it
            if (asyncResult._exception != null)
            {
                MethodInfo mth = typeof (Exception).GetMethod("InternalPreserveStackTrace", BindingFlags.Instance | BindingFlags.NonPublic);
                if (mth != null)
                    mth.Invoke(asyncResult._exception, null);
                throw asyncResult._exception;
            }
        }

        #region Implementation of IAsyncResult

        public object AsyncOwner
        {
            get { return _owner; }
        }

        public object AsyncSender
        {
            get { return _sender; }
        }

        public object Tag
        {
            get { return _tag; }
        }

        public object AsyncState
        {
            get { return _asyncState; }
        }

        public bool CompletedSynchronously
        {
            get
            {
                return Thread.VolatileRead(ref _completedState) ==
                       StateCompletedSynchronously;
            }
        }

        public WaitHandle AsyncWaitHandle
        {
            get { return _asyncWaitHandle.WaitHandle; }
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
}