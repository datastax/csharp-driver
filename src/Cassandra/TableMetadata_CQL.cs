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
                cql.AppendFormat("{0} {1}, \n", column.Name, GetColumnType(column));
            }

            List<string> partitionKeys = (from p in PartitionKeys
                                         select p.Name).ToList();

            if (partitionKeys.Count() > 0)
            {
                cql.Append("PRIMARY KEY(");

                if (partitionKeys.Count() > 1)
                    cql.AppendFormat("({0})", string.Join(",", partitionKeys));
                else if (partitionKeys.Count() == 1)
                    cql.AppendFormat("{0}", partitionKeys[0]);

                if(ClusteringKeys.Length > 0)
                {
                    cql.AppendFormat(", ");

                    var clusterNames = (from c in ClusteringKeys
                                         select string.Format("{0}", c.Item1.Name)).ToList();

                    if (clusterNames.Count() > 1)
                        cql.AppendFormat("{0}", string.Join(",", clusterNames));
                    else if (clusterNames.Count() == 1)
                        cql.AppendFormat("{0}", clusterNames[0]);
                }

                cql.Append(")\n");
            }

            cql.Append(")");

            if(ClusteringKeys.Length > 0)
            {
                var clusterStrings = from c in ClusteringKeys
                                     select string.Format("{0} {1}", c.Item1.Name, c.Item2 == SortOrder.Ascending ? "ASC":"DESC");
                cql.AppendFormat("WITH CLUSTERING ORDER BY ({0})", string.Join(",", clusterStrings.ToList()));
            }

            // Options need to be formatted differently.  Leave them out for now.
            //cql.Append(Options.ToString());

            cql.Append(";\n");

            return cql.ToString();
        }

        private string GetColumnType(TableColumn column)
        {
            if (column.TypeInfo != null)
            {
                Type aggregateType = column.TypeInfo.GetType();
                if(aggregateType.Name == "ListColumnInfo")
                {
                   ColumnTypeCode listInnerType = ((ListColumnInfo)column.TypeInfo).ValueTypeCode;
                   return string.Format("list<{0}>", listInnerType.ToString().ToLower());
                }
                else if(aggregateType.Name == "SetColumnInfo")
                {
                    ColumnTypeCode listInnerType = ((ListColumnInfo)column.TypeInfo).ValueTypeCode;
                    return string.Format("set<{0}>", listInnerType.ToString().ToLower());
                }
                else if(aggregateType.Name == "MapColumnInfo")
                {
                    ColumnTypeCode keyType = ((MapColumnInfo)column.TypeInfo).KeyTypeCode;
                    ColumnTypeCode valueType = ((MapColumnInfo)column.TypeInfo).ValueTypeCode;

                    return string.Format("map<{0},{1}>", keyType.ToString().ToLower(), valueType.ToString().ToLower());
                }
            }

            // Default if not an aggregate type
            return column.TypeCode.ToString().ToLower();
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
