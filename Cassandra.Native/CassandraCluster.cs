using System.Collections.Generic;
using System.Net;
using System.Threading;

namespace Cassandra
{
    /// <summary>
    ///  Informations and known state of a Cassandra cluster. <p> This is the main
    ///  entry point of the driver. A simple example of access to a Cassandra cluster
    ///  would be: 
    /// <pre> Cluster cluster = Cluster.NewBuilder.AddContactPoint("192.168.0.1").Build(); 
    ///  Session session = Cluster.Connect("db1"); 
    ///  foreach (var row in session.execute("SELECT * FROM table1")) 
    ///    //do something ... </pre> 
    ///  <p> A cluster object maintains a
    ///  permanent connection to one of the cluster node that it uses solely to
    ///  maintain informations on the state and current topology of the cluster. Using
    ///  the connection, the driver will discover all the nodes composing the cluster
    ///  as well as new nodes joining the cluster.
    /// </summary>
    public class Cluster
    {
        public const int DefaultPort = 9042;

        readonly IEnumerable<IPAddress> _contactPoints;
        readonly int _port;
        readonly Policies _policies;
        readonly IAuthInfoProvider _credentialsDelegate = null;
        readonly bool _noBufferingIfPossible;

        readonly PoolingOptions _poolingOptions = new PoolingOptions();
        public PoolingOptions PoolingOptions { get { return _poolingOptions; } }

        readonly CompressionType _compression = CompressionType.NoCompression;
        public CompressionType Compression { get { return _compression; } }

        readonly string _defaultKeyspace = "";
        public string DefaultKeyspace { get { return _defaultKeyspace; } }

        readonly int _abortTimeout = Timeout.Infinite;

        private Cluster(IEnumerable<IPAddress> contactPoints, int port, Policies policies, IAuthInfoProvider credentialsDelegate = null, bool noBufferingIfPossible = false, CompressionType compression = CompressionType.NoCompression, string defaultKeyspace = "", int abortTimeout=Timeout.Infinite)
        {
            this._contactPoints = contactPoints;
            this._port = port;
            this._policies = policies;
            this._credentialsDelegate = credentialsDelegate;
            this._noBufferingIfPossible = noBufferingIfPossible;
            this._compression = compression;
            this._defaultKeyspace = defaultKeyspace;
            this._abortTimeout = abortTimeout;
        }

        /// <summary>
        ///  Build a new cluster based on the provided initializer. <p> Note that for
        ///  building a cluster programmatically, Cluster.NewBuilder provides a slightly less
        ///  verbose shortcut with <link>NewBuilder#Build</link>. <p> Also note that that all
        ///  the contact points provided by <code>* initializer</code> must share the same
        ///  port.
        /// </summary>
        /// <param name="initializer"> the Cluster.Initializer to use </param>
        /// 
        /// <returns>the newly created Cluster instance </returns>
        public static Cluster BuildFrom(IInitializer initializer)
        {
            IEnumerable<IPAddress> contactPoints = initializer.ContactPoints;
            //if (contactPoints.)
            //    throw new IllegalArgumentException("Cannot build a cluster without contact points");

            return new Cluster(contactPoints, initializer.Port, initializer.Policies, initializer.AuthInfoProvider, initializer.UseNoBufferingIfPossible, initializer.CompressionType, initializer.DefaultKeyspace, initializer.AbortTimeout);
        }

        /// <summary>
        ///  Creates a new <link>Cluster.NewBuilder</link> instance. <p> This is a shortcut
        ///  for <code>new Cluster.NewBuilder()</code>.
        /// </summary>
        /// 
        /// <returns>the new cluster builder.</returns>
        public static Builder Builder()
        {
                return new Builder();
        }

        /// <summary>
        ///  Creates a new session on this cluster.
        /// </summary>
        /// 
        /// <returns>a new session on this cluster sets to no keyspace.</returns>
        public Session Connect()
        {
            return Connect(_defaultKeyspace);
        }

        /// <summary>
        ///  Creates a new session on this cluster and sets a keyspace to use.
        /// </summary>
        /// <param name="keyspace"> The name of the keyspace to use for the created <code>Session</code>. </param>
        /// 
        /// <returns>a new session on this cluster sets to keyspace
        ///  <code>keyspaceName</code>. </returns>
        public Session Connect(string keyspace)
        {
            return new Session(
                clusterEndpoints: _contactPoints,
                port: _port,
                keyspace: keyspace,
                credentialsDelegate: _credentialsDelegate,
                policies: _policies,
                poolingOptions: _poolingOptions,
                noBufferingIfPossible: _noBufferingIfPossible,
                compression: _compression,
                abortTimeout: _abortTimeout
                );
        }

        public Session ConnectAndCreateDefaultKeyspaceIfNotExists()
        {
            var session = Connect("");
            try
            {
                session.ChangeKeyspace(_defaultKeyspace);
            }
            catch (InvalidException)
            {
                session.CreateKeyspaceIfNotExists(_defaultKeyspace);
                session.ChangeKeyspace(_defaultKeyspace);
            }
            return session;
        }
    }

    /// <summary>
    ///  Initializer for <link>Cluster</link> instances. <p> If you want to create a
    ///  new <code>Cluster</code> instance programmatically, then it is advised to use
    ///  <link>Cluster.Builder</link> (obtained through the
    ///  <link>Cluster#builder</link> method). <p> But it is also possible to
    ///  implement a custom <code>Initializer</code> that retrieve initialization from
    ///  a web-service or from a configuration file for instance.
    /// </summary>
    public interface IInitializer
    {

        /// <summary>
        ///  Gets the initial Cassandra hosts to connect to.
        /// </summary>
        IEnumerable<IPAddress> ContactPoints { get; }

        int Port { get; }

        Policies Policies { get; }

        IAuthInfoProvider AuthInfoProvider { get; }

        bool UseNoBufferingIfPossible { get; }

        string DefaultKeyspace { get; }

        CompressionType CompressionType { get; }

        int AbortTimeout { get; }
    }

    /// <summary>
    ///  Helper class to build <link>Cluster</link> instances.
    /// </summary>
    public class Builder : IInitializer
    {

        private readonly List<IPAddress> _addresses = new List<IPAddress>();
        private int _port = Cluster.DefaultPort;
        private IAuthInfoProvider _authProvider = null;
        private CompressionType _compression = CompressionType.NoCompression;

        private ILoadBalancingPolicy _loadBalancingPolicy;
        private IReconnectionPolicy _reconnectionPolicy;
        private IRetryPolicy _retryPolicy;
        private bool _noBufferingIfPossible = false;

        private string _defaultKeyspace = null;

        private int _abortTimeout = Timeout.Infinite;

        public IEnumerable<IPAddress> ContactPoints
        {
            get
            {
                return _addresses;
            }
        }

        public int Port { get {return _port;} }

        public Builder WithConnectionString(string connectionString)
        {
            var cnb = new ConnectionStringBuilder(connectionString);

            foreach (var addr in cnb.ContactPoints)
                AddContactPoints(addr);
            WithPort(cnb.Port);
            WithCompression(cnb.CompressionType);
            WithDefaultKeyspace(cnb.Keyspace);
            return this;
        }

        public Builder WithConnectionTimeout(int abortTimeout)
        {
            this._abortTimeout = abortTimeout;
            return this;
        }


        public Builder WithDefaultKeyspace(string defaultKeyspace)
        {
            this._defaultKeyspace = defaultKeyspace;
            return this;
        }

        /// <summary>
        ///  The port to use to connect to the Cassandra host. If not set through this
        ///  method, the default port (9042) will be used instead.
        /// </summary>
        /// <param name="port"> the port to set. </param>
        /// 
        /// <returns>this Builder</returns>
        public Builder WithPort(int port)
        {
            this._port = port;
            return this;
        }


        /// <summary>
        ///  Sets the compression to use for the transport.
        /// </summary>
        /// <param name="compression"> the compression to set </param>
        /// 
        /// <returns>this Builder <see>ProtocolOptions.Compression</returns>
        public Builder WithCompression(CompressionType compression)
        {
            this._compression = compression;
            return this;
        }

        public Builder OmmitBufferingIfPossible()
        {
            this._noBufferingIfPossible = true;
            return this;
        }

        /// <summary>
        ///  Adds a contact point. Contact points are addresses of Cassandra nodes that
        ///  the driver uses to discover the cluster topology. Only one contact point is
        ///  required (the driver will retrieve the address of the other nodes
        ///  automatically), but it is usually a good idea to provide more than one
        ///  contact point, as if that unique contact point is not available, the driver
        ///  won't be able to initialize itself correctly.'
        /// </summary>
        /// <param name="address"> the address of the node to connect to </param>
        /// 
        /// <returns>this Builder </returns>
        public Builder AddContactPoint(string address)
        {
            this._addresses.AddRange(Utils.ResolveHostByName(address));
            return this;
        }

        /// <summary>
        ///  Add contact points. See <link>Builder#addContactPoint</link> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="addresses"> addresses of the nodes to add as contact point
        ///  </param>
        /// 
        /// <returns>this Builder </returns>
        public Builder AddContactPoints(params string[] addresses)
        {
            foreach (string address in addresses)
                AddContactPoint(address);
            return this;
        }

        /// <summary>
        ///  Add contact points. See <link>Builder#addContactPoint</link> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="addresses"> addresses of the nodes to add as contact point
        ///  </param>
        /// 
        /// <returns>this Builder <see>Builder#addContactPoint</returns>
        public Builder AddContactPoints(params IPAddress[] addresses)
        {
            foreach (var address in addresses)
                this._addresses.Add(address);
            return this;
        }

        /// <summary>
        ///  Configure the load balancing policy to use for the new cluster. <p> If no
        ///  load balancing policy is set through this method,
        ///  <link>Policies#DefaultLoadBalancingPolicy</link> will be used instead.
        /// </summary>
        /// <param name="policy"> the load balancing policy to use </param>
        /// 
        /// <returns>this Builder</returns>
        public Builder WithLoadBalancingPolicy(ILoadBalancingPolicy policy)
        {
            this._loadBalancingPolicy = policy;
            return this;
        }

        /// <summary>
        ///  Configure the reconnection policy to use for the new cluster. <p> If no
        ///  reconnection policy is set through this method,
        ///  <link>Policies#DefaultReconnectionPolicy</link> will be used instead.
        /// </summary>
        /// <param name="policy"> the reconnection policy to use </param>
        /// 
        /// <returns>this Builder</returns>
        public Builder WithReconnectionPolicy(IReconnectionPolicy policy)
        {
            this._reconnectionPolicy = policy;
            return this;
        }

        /// <summary>
        ///  Configure the retry policy to use for the new cluster. <p> If no retry policy
        ///  is set through this method, <link>Policies#DefaultRetryPolicy</link> will
        ///  be used instead.
        /// </summary>
        /// <param name="policy"> the retry policy to use </param>
        /// 
        /// <returns>this Builder</returns>
        public Builder WithRetryPolicy(IRetryPolicy policy)
        {
            this._retryPolicy = policy;
            return this;
        }


        public Policies Policies
        {
            get
            {
                return new Policies(
                    _loadBalancingPolicy ?? Cassandra.Policies.DefaultLoadBalancingPolicy,
                    _reconnectionPolicy ?? Cassandra.Policies.DefaultReconnectionPolicy,
                    _retryPolicy ?? Cassandra.Policies.DefaultRetryPolicy
                );
            }
        }

        /// <summary>
        ///  Use the provided <code>AuthInfoProvider</code> to connect to Cassandra hosts.
        ///  <p> This is optional if the Cassandra cluster has been configured to not
        ///  require authentication (the default).
        /// </summary>
        /// <param name="authInfoProvider"> the authentication info provider to use
        ///  </param>
        /// 
        /// <returns>this Builder</returns>
        public Builder WithAuthInfoProvider(IAuthInfoProvider authInfoProvider)
        {
            this._authProvider = authInfoProvider;
            return this;
        }

        public IAuthInfoProvider AuthInfoProvider
        {
            get
            {
                return this._authProvider;
            }
        }

        public bool UseNoBufferingIfPossible
        {
            get
            {
                return _noBufferingIfPossible;
            }
        }

        /// <summary>
        ///  Build the cluster with the configured set of initial contact points and
        ///  policies. This is a shorthand for <code>Cluster.buildFrom(this)</code>.
        /// </summary>
        /// 
        /// <returns>the newly build Cluster instance. </returns>
        public Cluster Build()
        {
            return Cluster.BuildFrom(this);
        }

        public string DefaultKeyspace
        {
            get { return _defaultKeyspace; }
        }

        public CompressionType CompressionType
        {
            get { return _compression; }
        }


        public int AbortTimeout
        {
            get { return _abortTimeout; }
        }
    }
}
