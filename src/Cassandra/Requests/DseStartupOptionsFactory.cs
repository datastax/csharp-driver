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

namespace Cassandra.Requests
{
    internal class DseStartupOptionsFactory : IStartupOptionsFactory
    {
        public const string ApplicationNameOption = "APPLICATION_NAME";
        public const string ApplicationVersionOption = "APPLICATION_VERSION";
        public const string ClientIdOption = "CLIENT_ID";

        private readonly string _appName;
        private readonly string _appVersion;
        private readonly Guid _clusterId;
        private readonly IStartupOptionsFactory _startupOptionsFactory;

        public DseStartupOptionsFactory(Guid clusterId, string appVersion, string appName)
        {
            _appName = appName;
            _appVersion = appVersion;
            _clusterId = clusterId;
            _startupOptionsFactory = new StartupOptionsFactory();
        }

        public IReadOnlyDictionary<string, string> CreateStartupOptions(ProtocolOptions options)
        {
            var startupOptions = _startupOptionsFactory.CreateStartupOptions(options).ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

            if (_appName != null)
            {
                startupOptions[DseStartupOptionsFactory.ApplicationNameOption] = _appName;
            }

            if (_appVersion != null)
            {
                startupOptions[DseStartupOptionsFactory.ApplicationVersionOption] = _appVersion;
            }
            
            startupOptions[DseStartupOptionsFactory.ClientIdOption] = _clusterId.ToString();
            return startupOptions;
        }
    }
}