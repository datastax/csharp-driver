using System.Collections.Generic;

namespace Cassandra
{
    public static class ReplicationStrategies
    {
        public const string NetworkTopologyStrategy = "NetworkTopologyStrategy";
        public const string SimpleStrategy = "SimpleStrategy";


        /// <summary>
        ///  Returns replication property for SimpleStrategy.
        /// </summary>        
        /// <param name="replication_factor">Replication factor for the whole cluster.</param>
        /// <returns>a dictionary of replication property sub-options.</returns>         
        public static Dictionary<string, string> CreateSimpleStrategyReplicationProperty(int replication_factor)
        {
            return new Dictionary<string, string> { { "class", SimpleStrategy }, { "replication_factor", replication_factor.ToString() } };
        }


        /// <summary>
        ///  Returns replication property for NetworkTopologyStrategy.
        /// </summary>        
        /// <param name="datacenters_replication_factors">Dictionary in which key is the name of a data-center,
        /// value is a replication factor for that data-center.</param>
        /// <returns>a dictionary of replication property sub-options.</returns>         
        public static Dictionary<string, string> CreateNetworkTopologyStrategyReplicationProperty(Dictionary<string, int> datacenters_replication_factors)
        {
            Dictionary<string, string> result = new Dictionary<string, string> { { "class", NetworkTopologyStrategy } };
            if (datacenters_replication_factors.Count > 0)
                foreach (var datacenter in datacenters_replication_factors)
                    result.Add(datacenter.Key, datacenter.Value.ToString());
            return result;
        }


        /// <summary>
        ///  Returns replication property for other replication strategy. 
        ///  Use it only if there is no dedicated method that creates replication property for specified replication strategy.
        /// </summary>
        /// <param name="strategy_class">Name of replication strategy.</param>
        /// <param name="sub_options">Dictionary in which key is the name of sub-option,
        /// value is a value for that sub-option.</param>
        /// <returns>a dictionary of replication property sub-options.</returns>         
        public static Dictionary<string, string> CreateReplicationProperty(string strategy_class, Dictionary<string, string> sub_options)
        {
            Dictionary<string, string> result = new Dictionary<string, string> { { "class", strategy_class } };
            if (sub_options.Count > 0)
                foreach (var elem in sub_options)
                    result.Add(elem.Key, elem.Value);
            return result;
        }
    }
}