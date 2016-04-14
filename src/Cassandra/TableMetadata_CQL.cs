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
        public string AsTableCql()
        {
            StringBuilder cql = new StringBuilder();

            cql.AppendFormat("CREATE TABLE {0} ( \n", CqlQueryTools.QuoteIdentifier(Name));

            foreach (TableColumn column in TableColumns)
            {
                cql.AppendFormat("{0} {1}, \n", column.Name, column.TypeCode);
            }

            var partitionKeys = from p in PartitionKeys
                                select p.Name;
            cql.AppendFormat("PRIMARY KEY ({0})\n", string.Join(",", partitionKeys.ToList()));

            cql.Append(")");

            if(ClusteringKeys.Length > 0)
            {
                var clusterStrings = from c in ClusteringKeys
                                     select string.Format("{0} {1}", c.Item1.Name, c.Item2.ToString());
                cql.AppendFormat("WITH CLUSTERING ORDER BY ({0})", string.Join(",", clusterStrings.ToList()));
            }

            // Options need to be formatted differently.  Leave them out for now.
            //cql.Append(Options.ToString());

            cql.Append(";\n");

            return cql.ToString();
        }

        public List<string> AsSecondaryIndexCql()
        {
            List<string> indexCQL = new List<string>();

            if(Indexes.Count > 0)
            {
                foreach(string indexName in Indexes.Keys)
                {
                    IndexMetadata index = Indexes[indexName];
                    indexCQL.Add(string.Format("CREATE INDEX ON {0} ({1});", Name, index.Target));
                }
            }

            return indexCQL;
        }
    }
}
