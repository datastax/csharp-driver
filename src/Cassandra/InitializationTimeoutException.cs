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
using System.Runtime.Serialization;

namespace Cassandra
{
    [Serializable]
    public class InitializationTimeoutException : DriverException
    {
        internal InitializationTimeoutException() : base("Timed out while waiting for cluster initialization to finish. This mechanism is put in place to" +
                                                         " avoid blocking the calling thread forever. This usually caused by a networking issue" +
                                                         " between the client driver instance and the cluster. You can increase this timeout via " +
                                                         "the SocketOptions.ConnectTimeoutMillis config setting. This can also be related to deadlocks " +
                                                         "caused by mixing synchronous and asynchronous code.")
        {
        }

        internal InitializationTimeoutException(string message) : base(message)
        {
        }

        internal InitializationTimeoutException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected InitializationTimeoutException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}