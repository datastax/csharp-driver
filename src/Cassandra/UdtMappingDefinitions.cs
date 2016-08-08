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

﻿using System;
using System.Collections.Concurrent;
﻿using Cassandra.Mapping.Statements;
﻿using Cassandra.Serialization;

namespace Cassandra
{
    /// <summary>
    /// Allows configuration of user defined types.
    /// </summary>
    public class UdtMappingDefinitions
    {
        private readonly ConcurrentDictionary<Type, UdtMap> _udtByNetType;
        private readonly ICluster _cluster;
        private readonly ISession _session;
        private readonly Serializer _serializer;

        internal UdtMappingDefinitions(ISession session, Serializer serializer)
        {
            _udtByNetType = new ConcurrentDictionary<Type, UdtMap>();
            _cluster = session.Cluster;
            _session = session;
            _serializer = serializer;
        }

        /// <summary>
        /// Add mapping definition(s) for UDTs, specifying how UDTs should be mapped to .NET types and vice versa.
        /// </summary>
        /// <exception cref="ArgumentException" />
        public void Define(params UdtMap[] udtMaps)
        {
            if (udtMaps == null)
            {
                throw new ArgumentNullException("udtMaps");
            }
            var keyspace = _session.Keyspace;
            if (String.IsNullOrEmpty(keyspace))
            {
                throw new ArgumentException("It is not possible to define a mapping when no keyspace is specified.");
            }
            if (_session.BinaryProtocolVersion < 3)
            {
                throw new NotSupportedException("User defined type mapping is supported with C* 2.1+ and protocol version 3+");
            }
            // Add types to both indexes
            foreach (var map in udtMaps)
            {
                var udtDefition = GetDefinition(keyspace, map);
                map.SetSerializer(_serializer);
                map.Build(udtDefition);
                _serializer.SetUdtMap(udtDefition.Name, map);
                _udtByNetType.AddOrUpdate(map.NetType, map, (k, oldValue) => map);
            }
        }

        public void CreateAndDefine(params UdtMap[] udtMaps)
        {
            foreach (var map in udtMaps)
            {
                Create(map, false);
                Define(map);
            }
        }

        public void CreateIfNotExistsAndDefine(params UdtMap[] udtMaps)
        {
            foreach (var map in udtMaps)
            {
                Create(map, true);
                Define(map);
            }
        }

        private void Create(UdtMap map, bool ifNotExists)
        {
            _session.Execute(
                CqlGenerator.GetCreateUserDefinedType(_serializer, map.NetType,
                    UdtMap.PropertyFlags,
                    map.UdtName, ifNotExists));
        }

        /// <summary>
        /// Gets the definition and validates the fields
        /// </summary>
        /// <exception cref="InvalidTypeException" />
        private UdtColumnInfo GetDefinition(string keyspace, UdtMap map)
        {
            var caseSensitiveUdtName = map.UdtName;
            if (map.IgnoreCase)
            {
                //identifiers are lower cased in Cassandra
                caseSensitiveUdtName = caseSensitiveUdtName.ToLower();
            }
            var udtDefinition = _cluster.Metadata.GetUdtDefinition(keyspace, caseSensitiveUdtName);
            if (udtDefinition == null)
            {
                throw new InvalidTypeException(caseSensitiveUdtName + " UDT not found on keyspace " + keyspace);
            }
            return udtDefinition;
        }

        internal UdtMap GetUdtMap<T>(string keyspace)
        {
            return GetUdtMap(keyspace, typeof(T));
        }

        internal UdtMap GetUdtMap(string keyspace, Type netType)
        {
            UdtMap map;
            return _udtByNetType.TryGetValue(netType, out map) ? map : null;
        }
    }
}