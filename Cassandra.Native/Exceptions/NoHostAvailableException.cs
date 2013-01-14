using System;
using System.Collections.Generic;
using System.Net;

namespace Cassandra
{

    /// <summary>
    ///  Exception thrown when a query cannot be performed because no host are available. This exception is thrown if <ul> <li>either there is no host live in the cluster at the moment of the query</li> <li>all host that have been tried have failed due to a connection problem</li> </ul> For debugging purpose, the list of hosts that have been tried along with the failure cause can be retrieved using the {
    /// </summary>
    public class NoHostAvailableException : DriverException
    {

        /// <summary>
        ///  Gets the hosts tried along with descriptions of the error encountered while trying them. 
        /// </summary>
        public Dictionary<IPAddress, Exception> Errors { get; private set; }

        public NoHostAvailableException(Dictionary<IPAddress, Exception> errors)
            : base(MakeMessage(errors))
        {
            this.Errors = errors;
        }

        private static String MakeMessage(Dictionary<IPAddress, Exception> errors)
        {
            return string.Format("All host tried for query are in error (tried: {0})", string.Join(",", errors.Keys));
        }
    }
}