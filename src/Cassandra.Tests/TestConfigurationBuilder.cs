﻿// 
//       Copyright (C) 2019 DataStax Inc.
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
// 

using System.Collections.Generic;
using Cassandra.Connections;
using Cassandra.Requests;
using Cassandra.SessionManagement;

namespace Cassandra.Tests
{
    internal class TestConfigurationBuilder
    {
        public Policies Policies { get; set; } = Policies.DefaultPolicies;

        public ProtocolOptions ProtocolOptions { get; set; } = new ProtocolOptions();

        public PoolingOptions PoolingOptions { get; set; } = new PoolingOptions();

        public SocketOptions SocketOptions { get; set; } = new SocketOptions();

        public ClientOptions ClientOptions { get; set; } = new ClientOptions();

        public IAuthProvider AuthProvider { get; set; } = new NoneAuthProvider();

        public IAuthInfoProvider AuthInfoProvider { get; set; } = new SimpleAuthInfoProvider();

        public QueryOptions QueryOptions { get; set; } = new QueryOptions();

        public IAddressTranslator AddressTranslator { get; set; } = new DefaultAddressTranslator();

        public IStartupOptionsFactory StartupOptionsFactory { get; set; } = new StartupOptionsFactory();

        public ISessionFactoryBuilder<IInternalCluster, IInternalSession> SessionFactoryBuilder { get; set; } = new SessionFactoryBuilder();

        public IReadOnlyDictionary<string, ExecutionProfile> ExecutionProfiles { get; set; } = new Dictionary<string, ExecutionProfile>();

        public IRequestHandlerFactory RequestHandlerFactory { get; set; } = new RequestHandlerFactory();

        public IHostConnectionPoolFactory HostConnectionPoolFactory { get; set; } = new HostConnectionPoolFactory();

        public IRequestExecutionFactory RequestExecutionFactory { get; set; } = new RequestExecutionFactory();

        public IConnectionFactory ConnectionFactory { get; set; } = new ConnectionFactory();

        public Configuration Build()
        {
            return new Configuration(
                Policies,
                ProtocolOptions,
                PoolingOptions,
                SocketOptions,
                ClientOptions,
                AuthProvider,
                AuthInfoProvider,
                QueryOptions,
                AddressTranslator,
                StartupOptionsFactory,
                SessionFactoryBuilder,
                ExecutionProfiles,
                RequestHandlerFactory,
                HostConnectionPoolFactory,
                RequestExecutionFactory,
                ConnectionFactory);
        }
    }
}