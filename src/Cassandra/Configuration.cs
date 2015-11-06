//
//      Copyright (C) 2012-2014 DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using Cassandra.Tasks;
using Microsoft.IO;

namespace Cassandra
{
    /// <summary>
    ///  The configuration of the cluster. This handle setting: <ul> <li>Cassandra
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
        internal IAddressTranslator AddressTranslator
        {
            get { return _addressTranslator; }
        }

        /// <summary>
        /// Shared reusable timer
        /// </summary>
        internal HashedWheelTimer Timer { get; set; }

        /// <summary>
        /// Shared buffer pool
        /// </summary>
        internal RecyclableMemoryStreamManager BufferPool { get; set;}


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
        }

        /// <summary>
        /// Gets the pooling options. If not specified, gets the default by protocol version
        /// </summary>
        internal PoolingOptions GetPoolingOptions(byte protocolVersion)
        {
            if (this._poolingOptions != null)
            {
                return this._poolingOptions;
            }
            this._poolingOptions = PoolingOptions.GetDefault(protocolVersion);
            return this._poolingOptions;
        }
    }
}
