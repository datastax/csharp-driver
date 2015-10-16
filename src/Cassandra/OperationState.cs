//
//      Copyright (C) 2012-2014 DataStax Inc.
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

﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
﻿using System.Threading;
﻿using System.Threading.Tasks;
﻿using Cassandra.Requests;
﻿using Cassandra.Tasks;
﻿using Microsoft.IO;

namespace Cassandra
{
    /// <summary>
    /// Represents the state of the ongoing operation for the Connection
    /// </summary>
    internal class OperationState
    {
        private const int StateInit = 0;
        private const int StateCancelled = 1;
        private const int StateTimedout = 2;
        private const int StateCompleted = 3;
        private static readonly Action<Exception, AbstractResponse> Noop = (_, __) => { };

        private volatile Action<Exception, AbstractResponse> _callback;
        private readonly RecyclableMemoryStreamManager _bufferPool;
        private int _state = StateInit;
        /// <summary>
        /// Gets a readable stream representing the body
        /// </summary>
        public Stream BodyStream { get; private set; }

        /// <summary>
        /// Returns true if there are enough data to parse body
        /// </summary>
        public bool IsBodyComplete
        {
            get 
            {
                return BodyStream.Length == Header.BodyLength;
            }
        }

        /// <summary>
        /// 8 byte header of the frame
        /// </summary>
        public FrameHeader Header { get; set; }

        public IRequest Request { get; set; }

        /// <summary>
        /// Read timeout associated with the request
        /// </summary>
        public HashedWheelTimer.ITimeout Timeout { get; set; }

        /// <summary>
        /// Creates a new operation state with the provided callback
        /// </summary>
        public OperationState(Action<Exception, AbstractResponse> callback, RecyclableMemoryStreamManager bufferPool)
        {
            _callback = callback;
            _bufferPool = bufferPool;
        }

        /// <summary>
        /// Appends to the body stream
        /// </summary>
        /// <returns>The total amount of bytes added</returns>
        public int AppendBody(byte[] value, int offset, int count)
        {
            if (Header == null)
            {
                throw new DriverInternalError("To add a response body you must specify the header");
            }
            if (BodyStream == null)
            {
                BodyStream = _bufferPool.GetStream(GetType().Name);
            }
            if (BodyStream.Position + count > Header.BodyLength)
            {
                count = Header.BodyLength - (int) BodyStream.Position;
            }
            BodyStream.Write(value, offset, count);
            return count;
        }

        /// <summary>
        /// Invokes the callback in a new thread using the default task scheduler, and marks this operation as completed.
        /// </summary>
        public void InvokeCallback(Exception ex, AbstractResponse response = null)
        {
            //Change the state
            Interlocked.Exchange(ref _state, StateCompleted);
            //Set the status before getting the callback
            Thread.MemoryBarrier();
            var callback = _callback;
            if (Timeout != null)
            {
                //Cancel it if it hasn't expired
                Timeout.Cancel();
            }
            if (response is ErrorResponse)
            {
                //Create an exception from the response error
                ex = ((ErrorResponse)response).Output.CreateException();
                response = null;
            }
            //Invoke the callback in a new thread in the thread pool
            //This way we don't let the user block on a thread used by the Connection
            Task.Factory.StartNew(() => callback(ex, response), CancellationToken.None, TaskCreationOptions.None, TaskScheduler.Default);
        }

        /// <summary>
        /// Marks this operation as timed-out, callbacks with the exception 
        /// and sets a handler when the response is received
        /// </summary>
        public bool SetTimedOut(OperationTimedOutException ex, Action onReceive)
        {
            var callback = _callback;
            //When the data is received, invoke on receive callback
            _callback = (_, __) => onReceive();
            //Set the _callback first, as the Invoke method does not check previous state
            Thread.MemoryBarrier();
            var previousState = Interlocked.Exchange(ref _state, StateTimedout);
            switch (previousState)
            {
                case StateInit:
                    //Call the original callback
                    Task.Factory.StartNew(() => callback(ex, null), CancellationToken.None, TaskCreationOptions.None, TaskScheduler.Default);
                    return true;
                case StateCompleted:
                    //it arrived while changing the state
                    //Invoke on receive
                    _callback = Noop;
                    //it hasn't actually timed out
                    return false;
            }
            //For cancelled, do not invoke the previous
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
            //If it was init and now is cancelled, change the final callback to a noop
            _callback = Noop;
            if (Timeout != null)
            {
                //Cancel it if it hasn't expired
                //We should not worry about yielding OperationTimedOutExceptions when this is cancelled.
                Timeout.Cancel();
            }
        }
    }
}
