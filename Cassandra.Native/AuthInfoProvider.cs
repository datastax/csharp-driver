using System.Collections.Generic;
using System.Net;

namespace Cassandra
{
    /**
     * Authentication informations provider to connect to Cassandra nodes.
     * <p>
     * The authentication information themselves are just a key-value pairs.
     * Which exact key-value pairs are required depends on the authenticator
     * set for the Cassandra nodes.
     */
    public interface IAuthInfoProvider
    {
        /**
         * The authentication informations to use to connect to {@code host}.
         *
         * Please note that if authentication is required, this method will be
         * called to initialize each new connection created by the driver. It is
         * thus a good idea to make sure this method returns relatively quickly.
         *
         * @param host the Cassandra host for which authentication information
         * are requested.
         * @return The authentication informations to use.
         */
         IDictionary<string, string> GetAuthInfos(IPAddress host);
    }
}
