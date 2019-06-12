//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Linq;
using System.Collections.Generic;
using System.Net;
#if NET45
using System.Runtime.Serialization;
#endif
using System.Text;

namespace Dse
{
    /// <summary>
    ///  Exception thrown when a query cannot be performed because no host are
    ///  available. This exception is thrown if <ul> <li>either there is no host live
    ///  in the cluster at the moment of the query</li> <li>all host that have been
    ///  tried have failed due to a connection problem</li> </ul> For debugging
    ///  purpose, the list of hosts that have been tried along with the failure cause
    ///  can be retrieved using the <link>#errors</link> method.
    /// </summary>
#if NET45
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

#if NET45
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
