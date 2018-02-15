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
using System;
using System.Linq;
using System.Collections.Generic;
using System.Net;
#if !NETCORE
using System.Runtime.Serialization;
#endif
using System.Text;

namespace Cassandra
{
    /// <summary>
    ///  Exception thrown when a query cannot be performed because no host are
    ///  available. This exception is thrown if <ul> <li>either there is no host live
    ///  in the cluster at the moment of the query</li> <li>all host that have been
    ///  tried have failed due to a connection problem</li> </ul> For debugging
    ///  purpose, the list of hosts that have been tried along with the failure cause
    ///  can be retrieved using the <link>#errors</link> method.
    /// </summary>
#if !NETCORE
    [Serializable]
#endif
    public class NoHostAvailableException : DriverException
    {
        private const string StartMessage = "All hosts tried for query failed (tried ";
        private const int MaxTriedInfo = 2;
        
        /// <summary>
        ///  Gets the hosts tried along with descriptions of the error encountered while trying them. 
        /// </summary>
        public Dictionary<IPEndPoint, Exception> Errors { get; }

        public NoHostAvailableException(Dictionary<IPEndPoint, Exception> errors)
            : base(CreateMessage(errors))
        {
            Errors = errors;
        }

        /// <summary>
        /// Creates a new instance of NoHostAvailableException with a custom message and an empty error dictionary. 
        /// </summary>
        internal NoHostAvailableException(string message) : base(message)
        {
            Errors = new Dictionary<IPEndPoint, Exception>(0);
        }

#if !NETCORE
        protected NoHostAvailableException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
            
        }
#endif

        private static string CreateMessage(Dictionary<IPEndPoint, Exception> errors)
        {
            if (errors.Count == 0)
            {
                return "No host is available to be queried (no host tried)";
            }
            var builder = new StringBuilder(StartMessage, StartMessage.Length + 128);
            var first = true;
            foreach (var kv in errors.Take(MaxTriedInfo))
            {
                if (!first)
                {
                    builder.Append("; ");   
                }
                builder.Append(kv.Key);
                if (kv.Value != null)
                {
                    builder.Append(": ");
                    builder.Append(kv.Value.GetType().Name);
                    builder.Append(" '");
                    builder.Append(kv.Value.Message);
                    builder.Append("'");   
                }
                first = false;
            }
            builder.Append(errors.Count <= MaxTriedInfo ? ")" : "; ...), see Errors property for more info");
            return builder.ToString();
        }
    }
}
