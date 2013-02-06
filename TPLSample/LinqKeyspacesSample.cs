using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;

namespace TPLSample.LinqKeyspacesSample
{

    public class SchemaColumns
    {
        public string keyspace_name { get; set; }

        public string columnfamily_name { get; set; }

        public string column_name { get; set; }

        public int component_index { get; set; }

        public string validator { get; set; }
    }

    public static class LinqKeyspacesSample
    {
        public static void Run()
        {
            Cluster cluster = Cluster.Builder().AddContactPoint("cassi.cloudapp.net").WithoutRowSetBuffering().WithoutRowSetBuffering().Build();

            using (var session = cluster.Connect("system"))
            {
                var context = new Context(session, ConsistencyLevel.One, ConsistencyLevel.One);
                context.AddTable<SchemaColumns>("schema_columns");

                var cqlKeyspaces = from t in context.GetTable<SchemaColumns>("schema_columns") 
                                   where t.keyspace_name == "system" select t;

                var req = Task<IEnumerable<SchemaColumns>>.Factory.FromAsync(cqlKeyspaces.BeginExecute,
                                                                   cqlKeyspaces.EndExecute, null).Result;

                DisplayResult(req);
            }

            cluster.Shutdown();
        }

        private static void DisplayResult(IEnumerable<SchemaColumns> req)
        {
            foreach (var schemaColumns in req)
            {
                Console.WriteLine("KeyspaceName={0} ColumnFamilyName={1} ColumnName={2}",
                                  schemaColumns.keyspace_name, schemaColumns.columnfamily_name, schemaColumns.column_name);
            }
        }
    }
}