//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Concurrent;
 ﻿using Dse.Serialization;using System.Threading.Tasks;
using Dse.Tasks;

namespace Dse
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
            TaskHelper.WaitToComplete(DefineAsync(udtMaps), _cluster.Configuration.ClientOptions.QueryAbortTimeout);
        }

        /// <summary>
        /// Add mapping definition(s) for UDTs, specifying how UDTs should be mapped to .NET types and vice versa.
        /// </summary>
        /// <exception cref="ArgumentException" />
        public async Task DefineAsync(params UdtMap[] udtMaps)
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
                var udtDefition = await GetDefinitionAsync(keyspace, map).ConfigureAwait(false);
                map.SetSerializer(_serializer);
                map.Build(udtDefition);
                _serializer.SetUdtMap(udtDefition.Name, map);
                _udtByNetType.AddOrUpdate(map.NetType, map, (k, oldValue) => map);
            }
        }

        /// <summary>
        /// Gets the definition and validates the fields
        /// </summary>
        /// <exception cref="InvalidTypeException" />
        private async Task<UdtColumnInfo> GetDefinitionAsync(string keyspace, UdtMap map)
        {
            var caseSensitiveUdtName = map.UdtName;
            if (map.IgnoreCase)
            {
                //identifiers are lower cased in Cassandra
                caseSensitiveUdtName = caseSensitiveUdtName.ToLower();
            }
            var udtDefinition = await _cluster.Metadata.GetUdtDefinitionAsync(keyspace, caseSensitiveUdtName).ConfigureAwait(false);
            if (udtDefinition == null)
            {
                throw new InvalidTypeException($"{caseSensitiveUdtName} UDT not found on keyspace {keyspace}");
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