using System.Collections.Generic;
using System.Net;
namespace Cassandra
{
    /**
     * A simple {@code AuthInfoProvider} implementation.
     * <p>
     * This provider allows to programmatically define authentication
     * information that will then apply to all hosts.
     * <p>
     * Note that it is <b>not</b> safe to add new info to this provider once a
     * Cluster instance has been created using this provider.
     */
    public class SimpleAuthInfoProvider : AuthInfoProvider
    {

        private Dictionary<string, string> credentials = new Dictionary<string, string>();

        /**
         * Creates a new, empty, simple authentication info provider.
         */
        public SimpleAuthInfoProvider() { }

        /**
         * Creates a new simple authentication info provider with the
         * informations contained in {@code properties}.
         *
         * @param properties a map of authentication information to use.
         */
        public SimpleAuthInfoProvider(Dictionary<string, string> properties)
        {
            addAll(properties);
        }

        public IDictionary<string, string> getAuthInfos(IPAddress host)
        {
            return credentials;
        }

        /**
         * Adds a new property to the authentication info returned by this
         * provider.
         *
         * @param property the name of the property to add.
         * @param value the value to add for {@code property}.
         * @return {@code this} object.
         */
        public SimpleAuthInfoProvider add(string property, string value)
        {
            credentials.Add(property, value);
            return this;
        }

        /**
         * Adds all the key-value pair provided as new authentication
         * information returned by this provider.
         *
         * @param properties a map of authentication information to add.
         * @return {@code this} object.
         */
        public SimpleAuthInfoProvider addAll(Dictionary<string, string> properties)
        {
            foreach (var kv in properties)
                credentials[kv.Key] = kv.Value;
            return this;
        }
    }
}
