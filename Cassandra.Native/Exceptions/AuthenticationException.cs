using System.Net;
namespace Cassandra
{
    /// <summary>
    /// Indicates an error during the authentication phase while connecting to a node.
    /// </summary>
    public class AuthenticationException : DriverUncheckedException
    {

        public IPAddress Host { get; private set; }

        public AuthenticationException(string Message, IPAddress Host)
            : base(string.Format("Authentication error on host {0}: {1}", Host, Message))
        {
            this.Host = Host;
        }
    }
}