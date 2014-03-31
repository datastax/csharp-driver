//
//      Copyright (C) 2012 DataStax Inc.
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

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;

//based on https://github.com/pchalamet/cassandra-sharp/tree/master/Samples

namespace TPLSample.LinqKeyspacesSample
{
    public static class LinqKeyspacesSample
    {
        public static void Run()
        {
            Cluster cluster = Cluster.Builder().AddContactPoint("127.0.0.1").WithoutRowSetBuffering().WithoutRowSetBuffering().Build();

            using (Session session = cluster.Connect("system"))
            {
                var context = new Context(session);
                context.AddTable<SchemaColumns>("schema_columns");

                CqlQuery<SchemaColumns> cqlKeyspaces = from t in context.GetTable<SchemaColumns>("schema_columns")
                                                       where t.keyspace_name == "system"
                                                       select t;

                IEnumerable<SchemaColumns> req = Task<IEnumerable<SchemaColumns>>.Factory.FromAsync(cqlKeyspaces.BeginExecute,
                                                                                                    cqlKeyspaces.EndExecute, null).Result;

                DisplayResult(req);
            }

            cluster.Shutdown();
        }

        private static void DisplayResult(IEnumerable<SchemaColumns> req)
        {
            foreach (SchemaColumns schemaColumns in req)
            {
                Console.WriteLine("KeyspaceName={0} ColumnFamilyName={1} ColumnName={2}",
                                  schemaColumns.keyspace_name, schemaColumns.columnfamily_name, schemaColumns.column_name);
            }
        }
    }
}