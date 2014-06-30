using System;
using System.Collections.Generic;

namespace Cassandra
{
    /// <summary>
    /// Allows configuration of user defined types.
    /// </summary>
    public class UdtMappingDefinitions
    {
        private readonly Dictionary<string, UdtMap> _udtByName;
        private readonly Dictionary<Type, UdtMap> _udtByNetType;
        private readonly ICluster _cluster;

        internal UdtMappingDefinitions(ICluster cluster)
        {
            _udtByName = new Dictionary<string, UdtMap>();
            _udtByNetType = new Dictionary<Type, UdtMap>();
            _cluster = cluster;
        }

        /// <summary>
        /// Add mapping definition(s) for UDTs, specifying how UDTs should be mapped to .NET types and vice versa.
        /// </summary>
        public void Define(string keyspace, params UdtMap[] udtMaps)
        {
            if (udtMaps == null) throw new ArgumentNullException("udtMaps");

            // Add types to both indexes
            foreach (UdtMap map in udtMaps)
            {
                var udtDefition = GetDefinition(keyspace, map);
                _udtByName.Add(map.UdtName, map);
                _udtByNetType.Add(map.NetType, map);
            }
        }

        private UdtColumnInfo GetDefinition(string keyspace, UdtMap map)
        {
            var udtDefinition = _cluster.Metadata.GetUdtDefinition(keyspace, map.UdtName);
            //TODO: Validate
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

        internal UdtMap GetUdtMap(string keyspace, string udtName)
        {
            UdtMap map;
            return _udtByName.TryGetValue(udtName, out map) ? map : null;
        }
    }
}