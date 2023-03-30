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

namespace Cassandra.ExecutionProfiles
{
    /// <inheritdoc />
    internal class ExecutionProfileOptions : IExecutionProfileOptions
    {
        private readonly Dictionary<string, IExecutionProfile> _profiles = new Dictionary<string, IExecutionProfile>();

        /// <inheritdoc />
        public IExecutionProfileOptions WithProfile(string name, Action<IExecutionProfileBuilder> profileBuildAction)
        {
            return WithProfile(name, BuildProfile(profileBuildAction));
        }
        
        /// <inheritdoc />
        public IExecutionProfileOptions WithDerivedProfile(string name, string baseProfile, Action<IExecutionProfileBuilder> profileBuildAction)
        {
            return WithDerivedProfile(name, baseProfile, BuildProfile(profileBuildAction));
        }

        private IExecutionProfileOptions WithProfile(string name, IExecutionProfile profile)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentNullException(nameof(name));
            }
            
            if (_profiles.ContainsKey(name))
            {
                throw new ArgumentException("There is already an execution profile with that name.");
            }

            _profiles[name] = profile ?? throw new ArgumentNullException(nameof(profile));
            return this;
        }

        private IExecutionProfileOptions WithDerivedProfile(string name, string baseProfile, IExecutionProfile profile)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentNullException(nameof(name));
            }

            if (_profiles.ContainsKey(name))
            {
                throw new ArgumentException("There is already an execution profile with that name.");
            }
            
            if (profile == null)
            {
                throw new ArgumentNullException(nameof(profile));
            }
            
            if (baseProfile == null)
            {
                throw new ArgumentNullException(nameof(baseProfile));
            }

            if (!_profiles.TryGetValue(baseProfile, out var baseProfileInstance))
            {
                throw new ArgumentException("The base Execution Profile must be added before the derived Execution Profile.");
            }
            
            _profiles[name] = new ExecutionProfile(baseProfileInstance, profile);
            return this;
        }

        public IReadOnlyDictionary<string, IExecutionProfile> GetProfiles()
        {
            return _profiles;
        }

        private IExecutionProfile BuildProfile(Action<IExecutionProfileBuilder> profileBuildAction)
        {
            if (profileBuildAction == null)
            {
                throw new ArgumentNullException(nameof(profileBuildAction));
            }

            var builder = new ExecutionProfileBuilder();
            profileBuildAction(builder);
            return builder.Build();
        }
    }
}