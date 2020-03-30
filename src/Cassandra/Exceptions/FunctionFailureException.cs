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

// ReSharper disable once CheckNamespace
namespace Cassandra
{
    /// <summary>
    /// Specifies a User defined function execution failure.
    /// </summary>
    public class FunctionFailureException : DriverException
    {
        /// <summary>
        /// Keyspace where the function is defined
        /// </summary>
        public string Keyspace { get; set; }

        /// <summary>
        /// Name of the function
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Name types of the arguments
        /// </summary>
        public string[] ArgumentTypes { get; set; }

        public FunctionFailureException(string message) : base(message)
        {
        }

        public FunctionFailureException(string message, Exception innerException) : base(message, innerException)
        {
        }
        
        protected FunctionFailureException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}
