using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Cassandra
{
    /// <summary>
    /// Allows configuration of user defined types.
    /// </summary>
    public class UdtMappingDefinitions
    {
        private readonly ConcurrentDictionary<string, UdtMap> _udtByName;
        private readonly ConcurrentDictionary<Type, UdtMap> _udtByNetType;
        private readonly ICluster _cluster;

        internal UdtMappingDefinitions(ICluster cluster)
        {
            _udtByName = new ConcurrentDictionary<string, UdtMap>();
            _udtByNetType = new ConcurrentDictionary<Type, UdtMap>();
            _cluster = cluster;
        }

        /// <summary>
        /// Add mapping definition(s) for UDTs, specifying how UDTs should be mapped to .NET types and vice versa.
        /// </summary>
        public void Define(string keyspace, params UdtMap[] udtMaps)
        {
            if (udtMaps == null) throw new ArgumentNullException("udtMaps");

            // Add types to both indexes
            foreach (var map in udtMaps)
            {
                UdtMap mapStored;
                //Try to save the round trip and the validation
                if (!_udtByNetType.TryGetValue(map.NetType, out mapStored))
                {
                    var udtDefition = GetDefinition(keyspace, map);
                    _udtByName.AddOrUpdate(udtDefition.Name, map, (k, oldValue) => oldValue);
                    _udtByNetType.AddOrUpdate(map.NetType, map, (k, oldValue) => oldValue);
                }
            }
        }

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
            foreach (var field in udtDefinition.Fields)
            {
                if (field.TypeCode == ColumnTypeCode.Udt)
                {
                    //We deal with nested UDTs later
                    continue;
                }
                var prop = map.GetPropertyForUdtField(field.Name);
                if (prop == null)
                {
                    //No mapping defined
                    //MAYBE: throw an exception
                    continue;
                }
                //Check if its assignable to and from
                var fieldTargetType = TypeInterpreter.GetDefaultTypeFromCqlType(field.TypeCode, field.TypeInfo);
                if (!prop.PropertyType.IsAssignableFrom(fieldTargetType))
                {
                    throw new InvalidTypeException(field.Name + " type is not assignable to " + prop.PropertyType.Name);
                }
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

        internal UdtMap GetUdtMap(string keyspace, string udtName)
        {
            UdtMap map;
            return _udtByName.TryGetValue(udtName, out map) ? map : null;
        }
    }
}