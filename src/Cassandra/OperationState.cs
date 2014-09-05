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
using System.Threading.Tasks;

namespace Cassandra
{
    /// <summary>
    /// Represents the state of the ongoing operation for the Connection
    /// </summary>
    internal class OperationState
    {
        private static Logger _logger = new Logger(typeof(OperationState));
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

        public Action<Exception, AbstractResponse> Callback { get; set; }

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
                    BodyStream = new MemoryStream(value, offset, this.Header.BodyLength, false, false);
                    return this.Header.BodyLength;
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

        public void InvokeCallback(Exception ex, AbstractResponse response = null)
        {
            if (response is ErrorResponse)
            {
                InvokeCallback(((ErrorResponse)response).Output.CreateException());
                return;
            }
            if (this.Callback == null)
            {
                _logger.Error("No callback for response");
                return;
            }
            this.Callback(ex, response);
        }
    }
}
