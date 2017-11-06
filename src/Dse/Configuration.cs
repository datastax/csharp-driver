//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using Dse.Serialization;
using Dse.Tasks;
using Microsoft.IO;

namespace Dse
{
    /// <summary>
    ///  The configuration of the cluster. It configures the following: <ul> <li>Cassandra
    ///  binary protocol level configuration (compression).</li> <li>Connection
    ///  pooling configurations.</li> <li>low-level tcp configuration options
    ///  (tcpNoDelay, keepAlive, ...).</li> </ul>
    /// </summary>
    public class Configuration
    {
        private readonly IAuthInfoProvider _authInfoProvider;
        private readonly IAuthProvider _authProvider;
        private readonly ClientOptions _clientOptions;
        private readonly Policies _policies;

        private PoolingOptions _poolingOptions;
        private readonly ProtocolOptions _protocolOptions;
        private readonly QueryOptions _queryOptions;
        private readonly SocketOptions _socketOptions;
        private readonly IAddressTranslator _addressTranslator;
        private readonly RecyclableMemoryStreamManager _bufferPool;
        private readonly HashedWheelTimer _timer;

        /// <summary>
        ///  Gets the policies set for the cluster.
        /// </summary>
        public Policies Policies
        {
            get { return _policies; }
        }

        /// <summary>
        ///  Gets the low-level tcp configuration options used (tcpNoDelay, keepAlive, ...).
        /// </summary>
        public SocketOptions SocketOptions
        {
            get { return _socketOptions; }
        }

        /// <summary>
        ///  The Cassandra binary protocol level configuration (compression).
        /// </summary>
        /// 
        /// <returns>the protocol options.</returns>
        public ProtocolOptions ProtocolOptions
        {
            get { return _protocolOptions; }
        }

        /// <summary>
        ///  The connection pooling configuration, defaults to null.
        /// </summary>
        /// <returns>the pooling options.</returns>
        public PoolingOptions PoolingOptions
        {
            get { return _poolingOptions; }
        }

        /// <summary>
        ///  The .net client additional options configuration.
        /// </summary>
        public ClientOptions ClientOptions
        {
            get { return _clientOptions; }
        }

        /// <summary>
        ///  The query configuration.
        /// </summary>
        public QueryOptions QueryOptions
        {
            get { return _queryOptions; }
        }

        /// <summary>
        ///  The authentication provider used to connect to the Cassandra cluster.
        /// </summary>
        /// 
        /// <returns>the authentication provider in use.</returns>
        internal IAuthProvider AuthProvider
            // Not exposed yet on purpose
        {
            get { return _authProvider; }
        }

        /// <summary>
        ///  The authentication provider used to connect to the Cassandra cluster.
        /// </summary>
        /// 
        /// <returns>the authentication provider in use.</returns>
        internal IAuthInfoProvider AuthInfoProvider
            // Not exposed yet on purpose
        {
            get { return _authInfoProvider; }
        }

        /// <summary>
        ///  The address translator used to translate Cassandra node address.
        /// </summary> 
        /// <returns>the address translator in use.</returns>
        public IAddressTranslator AddressTranslator
        {
            get { return _addressTranslator; }
        }

        /// <summary>
        /// Shared reusable timer
        /// </summary>
        internal HashedWheelTimer Timer
        {
            get { return _timer; }
        }

        /// <summary>
        /// Shared buffer pool
        /// </summary>
        internal RecyclableMemoryStreamManager BufferPool
        {
            get { return _bufferPool; }
        }

        /// <summary>
        /// Gets or sets the list of <see cref="TypeSerializer{T}"/> defined.
        /// </summary>
        internal IEnumerable<ITypeSerializer> TypeSerializers { get; set; }

        internal Configuration() :
            this(Policies.DefaultPolicies,
                 new ProtocolOptions(),
                 null,
                 new SocketOptions(),
                 new ClientOptions(),
                 NoneAuthProvider.Instance,
                 null,
                 new QueryOptions(),
                 new DefaultAddressTranslator())
        {
        }

        /// <summary>
        /// Creates a new instance. This class is also used to shareable a context across all instance that are created below one Cluster instance.
        /// One configuration instance per Cluster instance.
        /// </summary>
        internal Configuration(Policies policies,
                               ProtocolOptions protocolOptions,
                               PoolingOptions poolingOptions,
                               SocketOptions socketOptions,
                               ClientOptions clientOptions,
                               IAuthProvider authProvider,
                               IAuthInfoProvider authInfoProvider,
                               QueryOptions queryOptions,
                               IAddressTranslator addressTranslator)
        {
            if (addressTranslator == null)
            {
                throw new ArgumentNullException("addressTranslator");
            }
            if (queryOptions == null)
            {
                throw new ArgumentNullException("queryOptions");
            }
            _policies = policies;
            _protocolOptions = protocolOptions;
            _poolingOptions = poolingOptions;
            _socketOptions = socketOptions;
            _clientOptions = clientOptions;
            _authProvider = authProvider;
            _authInfoProvider = authInfoProvider;
            _queryOptions = queryOptions;
            _addressTranslator = addressTranslator;
            // Create the buffer pool with 16KB for small buffers and 256Kb for large buffers.
            // The pool does not eagerly reserve the buffers, so it doesn't take unnecessary memory
            // to create the instance.
            _bufferPool = new RecyclableMemoryStreamManager(16 * 1024, 256 * 1024, ProtocolOptions.MaximumFrameLength);
            _timer = new HashedWheelTimer();
        }

        /// <summary>
        /// Gets the pooling options. If not specified, gets the default by protocol version
        /// </summary>
        internal PoolingOptions GetPoolingOptions(ProtocolVersion protocolVersion)
        {
            if (_poolingOptions != null)
            {
                return _poolingOptions;
            }
            _poolingOptions = PoolingOptions.Create(protocolVersion);
            return _poolingOptions;
        }
    }
}
