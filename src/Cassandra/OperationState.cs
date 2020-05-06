//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using Cassandra.Connections;
using Cassandra.Observers.Abstractions;
using Cassandra.Requests;
using Cassandra.Responses;
using Cassandra.Serialization;
using Cassandra.Tasks;

namespace Cassandra
{
    /// <summary>
    /// Represents the state of the ongoing operation for the Connection
    /// </summary>
    internal class OperationState
    {
        private static readonly Logger Logger = new Logger(typeof(OperationState));

        private readonly IOperationObserver _operationObserver;
        private const int StateInit = 0;
        private const int StateCancelled = 1;
        private const int StateTimedout = 2;
        private const int StateCompleted = 3;
        private Action<IRequestError, Response, long> _callback;
        public static readonly Action<IRequestError, Response, long> Noop = (_, __, ___) => { };
        private volatile bool _timeoutCallbackSet;
        private int _state = StateInit;
        private volatile HashedWheelTimer.ITimeout _timeout;

        /// <summary>
        /// See docs for <see cref="IRequest.ResultMetadata"/>.
        /// </summary>
        public ResultMetadata ResultMetadata { get; }

        /// <summary>
        /// Note: This is set to NULL after this operation is written to a frame.
        /// </summary>
        public IRequest Request { get; private set; }

        /// <summary>
        /// Gets or sets the timeout in milliseconds for the request.
        /// </summary>
        public int TimeoutMillis { get; }

        /// <summary>
        /// Creates a new operation state with the provided callback
        /// </summary>
        public OperationState(Action<IRequestError, Response> callback, IRequest request, int timeoutMillis, IOperationObserver operationObserver)
        {
            Volatile.Write(ref _callback, (exception, response, timestamp) =>
            {
                try
                {
                    callback(exception, response);
                    operationObserver.OnOperationReceive(exception, response, timestamp);
                }
                catch (Exception ex)
                {
                    OperationState.Logger.Warning("Exception thrown inside an operation callback: {0}", ex.ToString());
                }
            });
            _operationObserver = operationObserver;
            Request = request;
            TimeoutMillis = timeoutMillis;
            ResultMetadata = request?.ResultMetadata;
        }

        /// <summary>
        /// Sets the read timeout associated with the request
        /// </summary>
        public void SetTimeout(HashedWheelTimer.ITimeout value)
        {
            _timeout = value;
        }

        public long WriteFrame(short streamId, MemoryStream memoryStream, ISerializer serializer, long timestamp)
        {
            var frameLength = Request.WriteFrame(streamId, memoryStream, serializer);
            _operationObserver.OnOperationSend(frameLength, timestamp);
            //We will not use the request any more, stop reference it.
            Request = null;
            return frameLength;
        }

        public bool CanBeWritten()
        {
            return Volatile.Read(ref _state) == OperationState.StateInit;
        }

        /// <summary>
        /// Marks this operation as completed and returns the callback.
        /// Note that the returned callback might be a reference to <see cref="Noop"/>, as the original callback
        /// might be already called.
        /// </summary>
        public Action<IRequestError, Response, long> SetCompleted()
        {
            var previousState = Interlocked.CompareExchange(ref _state, StateCompleted, StateInit);
            if (previousState == StateCancelled || previousState == StateCompleted)
            {
                return Noop;
            }
            Action<IRequestError, Response, long> callback;
            if (previousState == StateInit)
            {
                callback = Interlocked.Exchange(ref _callback, Noop);
                var timeout = _timeout;
                if (timeout != null)
                {
                    //Cancel it if it hasn't expired
                    timeout.Cancel();
                }
                return callback;
            }
            //Operation has timed out
            var spin = new SpinWait();
            while (!_timeoutCallbackSet)
            {
                //Wait for the timeout callback to be set
                spin.SpinOnce();
            }
            callback = Interlocked.Exchange(ref _callback, Noop);
            return callback;
        }

        /// <summary>
        /// Marks this operation as completed and invokes the callback with the exception using the default task scheduler.
        /// Its safe to call this method multiple times as the underlying callback will be invoked just once.
        /// </summary>
        public void InvokeCallback(IRequestError error, long timestamp)
        {
            var callback = SetCompleted();
            if (callback == Noop)
            {
                return;
            }
            //Invoke the callback in a new thread in the thread pool
            //This way we don't let the user block on a thread used by the Connection
            Task.Factory.StartNew(() => callback(error, null, timestamp), CancellationToken.None, TaskCreationOptions.None, TaskScheduler.Default);
        }

        /// <summary>
        /// Marks this operation as timed-out, callbacks with the exception
        /// and sets a handler when the response is received
        /// </summary>
        public bool MarkAsTimedOut(OperationTimedOutException ex, Action onReceive, long timestamp)
        {
            var previousState = Interlocked.CompareExchange(ref _state, StateTimedout, StateInit);
            if (previousState != StateInit)
            {
                return false;
            }
            //When the data is received, invoke on receive callback
            var callback = Interlocked.Exchange(ref _callback, (_, __, ___) => onReceive());

            Thread.MemoryBarrier();

            _timeoutCallbackSet = true;
            Task.Factory.StartNew(() => callback(RequestError.CreateClientError(ex, false), null, timestamp), CancellationToken.None, TaskCreationOptions.None, TaskScheduler.Default);
            return true;
        }

        /// <summary>
        /// Removes the context associated with this request, if possible
        /// </summary>
        public void Cancel()
        {
            if (Interlocked.CompareExchange(ref _state, StateCancelled, StateInit) != StateInit)
            {
                return;
            }
            //Remove the closure
            Volatile.Write(ref _callback, Noop);
            var timeout = _timeout;
            if (timeout != null)
            {
                //Cancel it if it hasn't expired
                //We should not worry about yielding OperationTimedOutExceptions when this is cancelled.
                timeout.Cancel();
            }
        }

        /// <summary>
        /// Asynchronously marks the provided operations as completed and invoke the callbacks with the exception.
        /// </summary>
        internal static void CallbackMultiple(IEnumerable<OperationState> ops, IRequestError error, long timestamp)
        {
            Task.Factory.StartNew(() =>
            {
                foreach (var state in ops)
                {
                    var callback = state.SetCompleted();
                    callback(error, null, timestamp);
                }
            }, CancellationToken.None, TaskCreationOptions.None, TaskScheduler.Default);
        }
    }
}