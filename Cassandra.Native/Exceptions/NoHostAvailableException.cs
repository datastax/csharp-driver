using System;
using System.Collections.Generic;
using System.Text;
using System.Net;

namespace Cassandra
{

    /// <summary>
    /// Exception thrown when a query cannot be performed because no host are
    /// available.
    /// 
    /// This exception is thrown if
    /// <ul>
    ///   <li>either there is no host live in the cluster at the moment of the query</li>
    ///   <li>all host that have been tried have failed due to a connection problem</li>
    /// </ul>
    /// 
    /// For debugging purpose, the list of hosts that have been tried along with the
    /// failure cause can be retrieved using the {@link #errors} method.
    /// </summary>
    public class NoHostAvailableException : DriverException
    {

        /// <summary>
        /// Gets the hosts tried along with descriptions of the error encountered
        /// while trying them.
        /// A map containing for each tried host a description of the error
        /// triggered when trying it.
        /// </summary>
        public Dictionary<IPAddress, Exception> Errors { get; private set; }

        public NoHostAvailableException(Dictionary<IPAddress, Exception> Errors)
            : base("All host tried for query are in error")
        {
            this.Errors = Errors;
        }

    }
}