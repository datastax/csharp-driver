using System.Net;

namespace Cassandra
{
    /// <summary>
    ///  A provider that provides no authentication capability. <p> This is only
    ///  useful as a placeholder when no authentication is to be used. </p>
    /// </summary>
    public class NoneAuthProvider : IAuthProvider
    {
        public static readonly NoneAuthProvider Instance = new NoneAuthProvider();

        public IAuthenticator NewAuthenticator(IPAddress host)
        {
            throw new AuthenticationException(
                string.Format("Host {0} requires authentication, but no authenticator found in Cluster configuration", host),
                host);
        }
    }
}