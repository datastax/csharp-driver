//
//      Copyright (C) DataStax Inc.
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
using System.Collections.Generic;
using System.Linq;
using Cassandra.Connections;
using Cassandra.ExecutionProfiles;
using Cassandra.ProtocolEvents;
using Cassandra.Requests;
using Cassandra.Serialization;
using Cassandra.SessionManagement;
using Cassandra.Tasks;

using Microsoft.IO;

namespace Cassandra
{
    /// <summary>
    ///  The configuration of the cluster. It configures the following: <ul> <li>Cassandra
    ///  binary protocol level configuration (compression).</li> <li>Connection
    ///  pooling configurations.</li> <li>low-level tcp configuration options
    ///  (tcpNoDelay, keepAlive, ...).</li> </ul>
    /// </summary>
    public class Configuration
    {
        internal const string DefaultExecutionProfileName = "default";

        /// <summary>
        ///  Gets the policies set for the cluster.
        /// </summary>
        public Policies Policies { get; }

        /// <summary>
        ///  Gets the low-level tcp configuration options used (tcpNoDelay, keepAlive, ...).
        /// </summary>
        public SocketOptions SocketOptions { get; private set; }

        /// <summary>
        ///  The Cassandra binary protocol level configuration (compression).
        /// </summary>
        ///
        /// <returns>the protocol options.</returns>
        public ProtocolOptions ProtocolOptions { get; private set; }

        /// <summary>
        ///  The connection pooling configuration, defaults to null.
        /// </summary>
        /// <returns>the pooling options.</returns>
        public PoolingOptions PoolingOptions { get; private set; }

        /// <summary>
        ///  The .net client additional options configuration.
        /// </summary>
        public ClientOptions ClientOptions { get; private set; }

        /// <summary>
        ///  The query configuration.
        /// </summary>
        public QueryOptions QueryOptions { get; private set; }

        /// <summary>
        ///  The authentication provider used to connect to the Cassandra cluster.
        /// </summary>
        /// <returns>the authentication provider in use.</returns>
        internal IAuthProvider AuthProvider { get; private set; } // Not exposed yet on purpose

        /// <summary>
        ///  The authentication provider used to connect to the Cassandra cluster.
        /// </summary>
        /// <returns>the authentication provider in use.</returns>
        internal IAuthInfoProvider AuthInfoProvider { get; private set; } // Not exposed yet on purpose

        /// <summary>
        ///  The address translator used to translate Cassandra node address.
        /// </summary>
        /// <returns>the address translator in use.</returns>
        public IAddressTranslator AddressTranslator { get; private set; }
        
        /// <summary>
        /// Gets a read only key value map of execution profiles that were configured with
        /// <see cref="Builder.WithExecutionProfiles"/>. The keys are execution profile names and the values
        /// are <see cref="IExecutionProfile"/> instances.
        /// </summary>
        public IReadOnlyDictionary<string, IExecutionProfile> ExecutionProfiles { get; }

        /// <summary>
        /// Shared reusable timer
        /// </summary>
        internal HashedWheelTimer Timer { get; private set; }

        /// <summary>
        /// Shared buffer pool
        /// </summary>
        internal RecyclableMemoryStreamManager BufferPool { get; private set; }

        /// <summary>
        /// Gets or sets the list of <see cref="TypeSerializer{T}"/> defined.
        /// </summary>
        internal IEnumerable<ITypeSerializer> TypeSerializers { get; set; }

        internal MetadataSyncOptions MetadataSyncOptions { get; }

        internal IStartupOptionsFactory StartupOptionsFactory { get; }

        internal ISessionFactoryBuilder<IInternalCluster, IInternalSession> SessionFactoryBuilder { get; }

        internal IRequestOptionsMapper RequestOptionsMapper { get; }
        
        internal IRequestHandlerFactory RequestHandlerFactory { get; }

        internal IHostConnectionPoolFactory HostConnectionPoolFactory { get; }

        internal IRequestExecutionFactory RequestExecutionFactory { get; }

        internal IConnectionFactory ConnectionFactory { get; }
        
        internal IControlConnectionFactory ControlConnectionFactory { get; }

        internal IPrepareHandlerFactory PrepareHandlerFactory { get; }

        internal ITimerFactory TimerFactory { get; }
        
        /// <summary>
        /// The key is the execution profile name and the value is the IRequestOptions instance
        /// built from the execution profile with that key.
        /// </summary>
        internal IReadOnlyDictionary<string, IRequestOptions> RequestOptions { get; }

        internal IRequestOptions DefaultRequestOptions => RequestOptions[Configuration.DefaultExecutionProfileName];

        internal Configuration() :
            this(Policies.DefaultPolicies,
                 new ProtocolOptions(),
                 null,
                 new SocketOptions(),
                 new ClientOptions(),
                 NoneAuthProvider.Instance,
                 null,
                 new QueryOptions(),
                 new DefaultAddressTranslator(),
                 new StartupOptionsFactory(),
                 new SessionFactoryBuilder(),
                 new Dictionary<string, IExecutionProfile>(),
                 new RequestOptionsMapper(),
                 null)
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
                               IAddressTranslator addressTranslator,
                               IStartupOptionsFactory startupOptionsFactory,
                               ISessionFactoryBuilder<IInternalCluster, IInternalSession> sessionFactoryBuilder,
                               IReadOnlyDictionary<string, IExecutionProfile> executionProfiles,
                               IRequestOptionsMapper requestOptionsMapper,
                               MetadataSyncOptions metadataSyncOptions,
                               IRequestHandlerFactory requestHandlerFactory = null,
                               IHostConnectionPoolFactory hostConnectionPoolFactory = null,
                               IRequestExecutionFactory requestExecutionFactory = null,
                               IConnectionFactory connectionFactory = null,
                               IControlConnectionFactory controlConnectionFactory = null,
                               IPrepareHandlerFactory prepareHandlerFactory = null,
                               ITimerFactory timerFactory = null)
        {
            AddressTranslator = addressTranslator ?? throw new ArgumentNullException(nameof(addressTranslator));
            QueryOptions = queryOptions ?? throw new ArgumentNullException(nameof(queryOptions));
            Policies = policies;
            ProtocolOptions = protocolOptions;
            PoolingOptions = poolingOptions;
            SocketOptions = socketOptions;
            ClientOptions = clientOptions;
            AuthProvider = authProvider;
            AuthInfoProvider = authInfoProvider;
            StartupOptionsFactory = startupOptionsFactory;
            SessionFactoryBuilder = sessionFactoryBuilder;
            RequestOptionsMapper = requestOptionsMapper;
            MetadataSyncOptions = metadataSyncOptions?.Clone() ?? new MetadataSyncOptions();

            RequestHandlerFactory = requestHandlerFactory ?? new RequestHandlerFactory();
            HostConnectionPoolFactory = hostConnectionPoolFactory ?? new HostConnectionPoolFactory();
            RequestExecutionFactory = requestExecutionFactory ?? new RequestExecutionFactory();
            ConnectionFactory = connectionFactory ?? new ConnectionFactory();
            ControlConnectionFactory = controlConnectionFactory ?? new ControlConnectionFactory();
            PrepareHandlerFactory = prepareHandlerFactory ?? new PrepareHandlerFactory();
            TimerFactory = timerFactory ?? new TaskBasedTimerFactory();
            
            RequestOptions = RequestOptionsMapper.BuildRequestOptionsDictionary(executionProfiles, policies, socketOptions, clientOptions, queryOptions);
            ExecutionProfiles = BuildExecutionProfilesDictionary(executionProfiles, RequestOptions);

            // Create the buffer pool with 16KB for small buffers and 256Kb for large buffers.
            // The pool does not eagerly reserve the buffers, so it doesn't take unnecessary memory
            // to create the instance.
            BufferPool = new RecyclableMemoryStreamManager(16 * 1024, 256 * 1024, ProtocolOptions.MaximumFrameLength);
            Timer = new HashedWheelTimer();
        }
        
        /// <summary>
        /// Clones (shallow) the provided execution profile dictionary and add the default profile if not there yet.
        /// </summary>
        private IReadOnlyDictionary<string, IExecutionProfile> BuildExecutionProfilesDictionary(
            IReadOnlyDictionary<string, IExecutionProfile> executionProfiles,
            IReadOnlyDictionary<string, IRequestOptions> requestOptions)
        {
            var executionProfilesDictionary = executionProfiles.ToDictionary(profileKvp => profileKvp.Key, profileKvp => profileKvp.Value);
            var defaultOptions = requestOptions[Configuration.DefaultExecutionProfileName];
            executionProfilesDictionary[Configuration.DefaultExecutionProfileName] = new ExecutionProfile(defaultOptions);
            return executionProfilesDictionary;
        }

        /// <summary>
        /// Gets the pooling options. If not specified, gets the default by protocol version
        /// </summary>
        internal PoolingOptions GetPoolingOptions(ProtocolVersion protocolVersion)
        {
            if (PoolingOptions != null)
            {
                return PoolingOptions;
            }

            PoolingOptions = PoolingOptions.Create(protocolVersion);
            return PoolingOptions;
        }
    }
}