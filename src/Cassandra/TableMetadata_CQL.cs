using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra
{
    public partial class TableMetadata
    {
        /// <summary>
        ///  Returns a CQL query representing this table. This method returns a single
        ///  'CREATE TABLE' query with the options corresponding to this name
        ///  definition.
        /// </summary>
        /// <returns>the 'CREATE TABLE' query corresponding to this name.
        /// </returns>
        public string AsCqlQuery()
        {

            return ReadTableToCQL();
#if false

            var sb = new StringBuilder();

            sb.Append("CREATE KEYSPACE ").Append(CqlQueryTools.QuoteIdentifier(Name)).Append(" WITH ");
            sb.Append("REPLICATION = { 'class' : '").Append(StrategyClass).Append("'");
            foreach (var rep in Replication)
            {
                if (rep.Key == "class")
                {
                    continue;
                }
                sb.Append(", '").Append(rep.Key).Append("': '").Append(rep.Value).Append("'");
            }
            sb.Append(" } AND DURABLE_WRITES = ").Append(DurableWrites);
            sb.Append(";");
            return sb.ToString();
#endif
        }

        private string ReadTableToCQL()
        {
            StringBuilder cql = new StringBuilder();

            cql.AppendFormat("CREATE TABLE {0} ( \n", CqlQueryTools.QuoteIdentifier(Name));

            AddColumns(cql);
            var partitionKeys = from p in PartitionKeys
                                select p.Name;
            cql.AppendFormat("PRIMARY KEY ({0})", string.Join(",", partitionKeys.ToList()));

            cql.Append("\n)");

            if(ClusteringKeys.Length > 0)
            {
                var clusterStrings = from c in ClusteringKeys
                                     select string.Format("{0} {1}", c.Item1.Name, c.Item2.ToString());

                cql.AppendFormat("WITH CLUSTERING ORDER BY ({0}) \n", string.Join(",", clusterStrings.ToList()));
            }

            cql.Append(Options.ToString());

            cql.Append(" \n);");

            return cql.ToString();
        }

        private void AddColumns(StringBuilder cql)
        {
            foreach(TableColumn column in TableColumns)
            {
                cql.AppendFormat("{0} {1}, \n", column.Name, column.TypeCode);
            }
        }

        private void AddIndexes(StringBuilder cql)
        {
          //  Dictionary<string, IndexMetadata> indexes = Indexes;

            cql.Append("This is where the Indexes go");
        }
    }
}
