// 
//       Copyright (C) DataStax Inc.
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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Cassandra.Connections.Control
{
    internal class SniContactPoint : IContactPoint
    {
        private static readonly Logger Logger = new Logger(typeof(SniContactPoint));

        private readonly SortedSet<string> _serverNames;
        private readonly ISniEndPointResolver _resolver;

        public SniContactPoint(SortedSet<string> serverNames, ISniEndPointResolver resolver)
        {
            if (serverNames == null)
            {
                throw new ArgumentNullException(nameof(serverNames), "Server Names in SniContactPoint can't be null.");
            }

            if (serverNames.Any(name => name == null))
            {
                SniContactPoint.Logger.Warning("Found null server names in SniContactPoint, skipping those.");
                serverNames = new SortedSet<string>(serverNames.Where(name => name != null));
            }

            if (serverNames.Count == 0)
            {
                throw new ArgumentException("No server names found in SniContactPoint.");
            }

            _serverNames = serverNames;
            _resolver = resolver ?? throw new ArgumentNullException(nameof(resolver));
        }

        public bool CanBeResolved => true;
        
        public override string ToString()
        {
            return StringRepresentation;
        }

        public string StringRepresentation => _resolver.SniOptions.StringRepresentation;

        public async Task<IEnumerable<IConnectionEndPoint>> GetConnectionEndPointsAsync(bool refreshCache)
        {
            if (refreshCache)
            {
                await _resolver.RefreshProxyResolutionAsync().ConfigureAwait(false);
            }

            var result = new List<IConnectionEndPoint>();
            foreach (var serverName in _serverNames)
            {
                result.Add(new SniConnectionEndPoint(await _resolver.GetNextEndPointAsync(false).ConfigureAwait(false), serverName, this));
            }

            return result;
        }

        private bool TypedEquals(SniContactPoint other)
        {
            return _serverNames.SetEquals(other._serverNames) && _resolver.Equals(other._resolver);
        }

        public bool Equals(IContactPoint other)
        {
            return Equals((object)other);
        }

        public override bool Equals(object obj)
        {
            if (object.ReferenceEquals(null, obj))
            {
                return false;
            }

            if (object.ReferenceEquals(this, obj))
            {
                return true;
            }

            if (obj is SniContactPoint typedObj)
            {
                return TypedEquals(typedObj);
            }

            return false;
        }

        public override int GetHashCode()
        {
            return Utils.CombineHashCode(_serverNames) * 23 + _resolver.GetHashCode();
        }
    }
}