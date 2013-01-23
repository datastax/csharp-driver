using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    public enum StrategyClass
    {
        Unknown = 0,
        SimpleStrategy = 1,
        NetworkTopologyStrategy = 2,
        OldNetworkTopologyStrategy = 3
    }

    public class KeyspaceMetadata
    {
        public string Name { get; private set; }
        public bool? DurableWrites { get; private set; }
        public StrategyClass StrategyClass { get; private set; }
        public ReadOnlyDictionary<string, int?> Replication { get; private set; }

        internal readonly AtomicValue<ReadOnlyDictionary<string, AtomicValue<TableMetadata>>> Tables =
            new AtomicValue<ReadOnlyDictionary<string, AtomicValue<TableMetadata>>>(null);

        internal KeyspaceMetadata(string name, bool? durableWrites, StrategyClass strategyClass,
                                  ReadOnlyDictionary<string, int?> replicationOptions)
        {
            Name = name;
            DurableWrites = durableWrites;
            StrategyClass = strategyClass;
            Replication = replicationOptions;
        }

        /// <summary>
        ///  Return a <code>String</code> containing CQL queries representing this
        ///  name and the table it contains. In other words, this method returns the
        ///  queries that would allow to recreate the schema of this name, along with
        ///  all its table. Note that the returned String is formatted to be human
        ///  readable (for some defintion of human readable at least).
        /// </summary>
        /// 
        /// <returns>the CQL queries representing this name schema as a code
        ///  String}.</returns>
        public string ExportAsString()
        {
            var sb = new StringBuilder();

            sb.Append(AsCqlQuery()).Append("\n");

            //foreach (var tm in Tables.Value.Values)
            //    sb.Append("\n").Append(tm.Value.exportAsString()).Append("\n");

            return sb.ToString();
        }

        /// <summary>
        ///  Returns a CQL query representing this name. This method returns a single
        ///  'CREATE KEYSPACE' query with the options corresponding to this name
        ///  definition.
        /// </summary>
        /// 
        /// <returns>the 'CREATE KEYSPACE' query corresponding to this name.
        ///  <see>#exportAsString</returns>
        public string AsCqlQuery()
        {
            var sb = new StringBuilder();

            sb.Append("CREATE KEYSPACE ").Append(CqlQueryTools.CqlIdentifier(Name)).Append(" WITH ");
            sb.Append("REPLICATION = { 'class' : '").Append(Replication["class"]).Append("'");
            foreach (var rep in Replication)
            {
                if (rep.Key == "class")
                    continue;
                sb.Append(", '").Append(rep.Key).Append("': '").Append(rep.Value).Append("'");
            }
            sb.Append(" } AND DURABLE_WRITES = ").Append(DurableWrites);
            sb.Append(";");
            return sb.ToString();
        }
    }

    
}
