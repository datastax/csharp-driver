using System.Collections.Generic;
using System.Net;

namespace Cassandra
{
    /// <summary>
    ///  Initializer for <link>Cluster</link> instances. <p> If you want to create a
    ///  new <c>Cluster</c> instance programmatically, then it is advised to use
    ///  <link>Cluster.Builder</link> (obtained through the
    ///  <link>Cluster#builder</link> method).</p> <p> But it is also possible to
    ///  implement a custom <c>Initializer</c> that retrieve initialization from
    ///  a web-service or from a configuration file for instance.</p>
    /// </summary>
    public interface IInitializer
    {
        /// <summary>
        ///  Gets the initial Cassandra hosts to connect to.See
        ///  <link>Builder.AddContactPoint</link> for more details on contact
        /// </summary>
        ICollection<IPAddress> ContactPoints { get; }

        /// <summary>
        ///  The configuration to use for the new cluster. <p> Note that some
        ///  configuration can be modified after the cluster initialization but some other
        ///  cannot. In particular, the ones that cannot be change afterwards includes:
        ///  <ul> <li>the port use to connect to Cassandra nodes (see
        ///  <link>ProtocolOptions</link>).</li> <li>the policies used (see
        ///  <link>Policies</link>).</li> <li>the authentication info provided (see
        ///  <link>Configuration</link>).</li> <li>whether metrics are enabled (see
        ///  <link>Configuration</link>).</li> </ul></p>
        /// </summary>
        Configuration GetConfiguration();
    }
}