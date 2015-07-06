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
﻿using Cassandra.Tasks;

namespace Cassandra
{
    /// <summary>
    /// Represents the state of the ongoing operation for the Connection
    /// </summary>
    internal class OperationState
    {
        private Action<Exception, AbstractResponse> _callback;
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
                if (BodyStream is MemoryStream)
                {
                    return true;
                }
                if (BodyStream is ListBackedStream)
                {
                    return BodyStream.Length == Header.BodyLength;
                }
                return false;
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
        public OperationState(Action<Exception, AbstractResponse> callback)
        {
            _callback = callback;
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
                if (Header.BodyLength <= count)
                {
                    //There is no need to copy the buffer: Use the inner buffer
                    BodyStream = new MemoryStream(value, offset, Header.BodyLength, false, false);
                    return Header.BodyLength;
                }
                BodyStream = new ListBackedStream();
            }
            if (BodyStream.Position + count > Header.BodyLength)
            {
                count = Header.BodyLength - (int) BodyStream.Position;
            }
            BodyStream.Write(value, offset, count);
            return count;
        }

        /// <summary>
        /// Invokes the callback in a new thread using the default task scheduler.
        /// </summary>
        /// <remarks>
        /// It only invokes the callback if it hasn't been invoked yet, replacing it for a new callback if necessary.
        /// </remarks>
        /// <returns>True</returns>
        public bool InvokeCallback(Exception ex, AbstractResponse response = null, Action<Exception, AbstractResponse> replaceCallback = null)
        {
            var callback = _callback;
            if (callback == null || Interlocked.CompareExchange(ref _callback, replaceCallback, callback) != callback)
            {
                //Callback has already been called, we are done here
                return false;
            }
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
            return true;
        }
    }
}
