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
using Cassandra.Responses;

namespace Cassandra.Connections
{
    internal class RequestError : IRequestError
    {
        private RequestError(Exception ex, bool isServerError, bool unsent)
        {
            Exception = ex;
            IsServerError = isServerError;
            Unsent = unsent;
        }
        
        /// <summary>
        /// Creates a server side request error based on a server error.
        /// </summary>
        public static IRequestError CreateServerError(ErrorResponse response)
        {
            return new RequestError(response.Output.CreateException(), true, false);
        }
        
        /// <summary>
        /// Creates a client side request error based on an exception.
        /// </summary>
        public static IRequestError CreateServerError(Exception ex)
        {
            return new RequestError(ex, true, false);
        }
        
        /// <summary>
        /// Creates a client side request error based on a exception.
        /// </summary>
        public static IRequestError CreateClientError(Exception ex, bool unsent)
        {
            return new RequestError(ex, false, unsent);
        }
        
        public Exception Exception { get; }

        public bool IsServerError { get; }

        public bool Unsent { get; }
    }
}