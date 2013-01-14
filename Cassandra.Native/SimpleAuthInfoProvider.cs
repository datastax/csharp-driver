using System.Collections.Generic;
using System.Net;

namespace Cassandra
{
    /// <summary>
    ///  A simple <code>AuthInfoProvider</code> implementation. <p> This provider
    ///  allows to programmatically define authentication information that will then
    ///  apply to all hosts. <p> Note that it is <b>not</b> safe to add new info to
    ///  this provider once a Cluster instance has been created using this provider.
    /// </summary>
    public class SimpleAuthInfoProvider : IAuthInfoProvider
    {

        private readonly Dictionary<string, string> _credentials = new Dictionary<string, string>();

        /// <summary>
        ///  Creates a new, empty, simple authentication info provider.
        /// </summary>
        public SimpleAuthInfoProvider() { }

        /// <summary>
        ///  Creates a new simple authentication info provider with the informations
        ///  contained in <code>properties</code>.
        /// </summary>
        /// <param name="properties"> a map of authentication information to use.</param>
        public SimpleAuthInfoProvider(Dictionary<string, string> properties)
        {
            AddAll(properties);
        }

        public IDictionary<string, string> GetAuthInfos(IPAddress host)
        {
            return _credentials;
        }

        /// <summary>
        ///  Adds a new property to the authentication info returned by this provider.
        /// </summary>
        /// <param name="property"> the name of the property to add. </param>
        /// <param name="value"> the value to add for <code>property</code>. </param>
        /// 
        /// <returns><code>this</code> object.</returns>
        public SimpleAuthInfoProvider Add(string property, string value)
        {
            _credentials.Add(property, value);
            return this;
        }

        /// <summary>
        ///  Adds all the key-value pair provided as new authentication information
        ///  returned by this provider.
        /// </summary>
        /// <param name="properties"> a map of authentication information to add. </param>
        /// 
        /// <returns><code>this</code> object.</returns>
        public SimpleAuthInfoProvider AddAll(Dictionary<string, string> properties)
        {
            foreach (var kv in properties)
                _credentials[kv.Key] = kv.Value;
            return this;
        }
    }
}
