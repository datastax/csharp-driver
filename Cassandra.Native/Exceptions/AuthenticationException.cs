using System.Net;
namespace Cassandra
{
    /// <summary>
    ///  Indicates an error during the authentication phase while connecting to a node.
    /// </summary>
    public class AuthenticationException : DriverException
    {
        /// <summary>
        ///  Gets the host for which the authentication failed. 
        /// </summary>
        public IPAddress Host { get; private set; }

        public AuthenticationException(string message)
            : base(message)
        {
        }

        public AuthenticationException(string message, IPAddress host)
            : base(string.Format("Authentication error on host {0}: {1}", host, message))
        {
            this.Host = host;
        }
    }
}