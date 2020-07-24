//
//      Copyright (C) DataStax Inc.
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
using System.Diagnostics;
using System.Threading.Tasks;
using Cassandra.DataStax.Graph;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using NUnit.Framework;

namespace Cassandra.IntegrationTests
{
    public abstract class BaseIntegrationTest : TestGlobals
    {
        protected BaseIntegrationTest()
        {
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Info;
        }

        protected const string ClassicSchemaGremlinQuery =
            "schema.propertyKey('name').Text().ifNotExists().create();\n" +
            "schema.propertyKey('age').Int().ifNotExists().create();\n" +
            "schema.propertyKey('lang').Text().ifNotExists().create();\n" +
            "schema.propertyKey('weight').Float().ifNotExists().create();\n" +
            "schema.vertexLabel('person').properties('name', 'age').ifNotExists().create();\n" +
            "schema.vertexLabel('software').properties('name', 'lang').ifNotExists().create();\n" +
            "schema.edgeLabel('created').properties('weight').connection('person', 'software').ifNotExists().create();\n" +
            "schema.edgeLabel('knows').properties('weight').connection('person', 'person').ifNotExists().create();\n" +
            "schema.propertyKey('title').Text().ifNotExists().create();\n" +
            "schema.propertyKey('tags').Text().multiple().ifNotExists().create();\n" +
            "schema.vertexLabel('movie').properties('title', 'tags').ifNotExists().create();\n";
        
        protected const string CoreSchemaGremlinQuery =
            "schema.vertexLabel('person')" +
                ".partitionBy('name', Text)" +
                ".property('age', Int)" +
                ".create();\n" +
            "schema.vertexLabel('software')" +
                ".partitionBy('name', Text)" +
                ".property('lang', Text)" +
                ".create();\n" +
            "schema.edgeLabel('created')" +
                ".from('person').to('software')" +
                ".property('weight', Float)" +
                ".create();\n" +
            "schema.edgeLabel('knows')" +
                ".from('person').to('person')" +
                ".property('weight', Float)" +
                ".create();\n" +
            "schema.vertexLabel('movie')" +
                ".partitionBy('title', Text)" +
                ".property('tags', listOf(Text))" +
                ".create();\n";
        
        /// <summary>
        /// Reference graph: http://www.tinkerpop.com/docs/3.0.0.M1/
        /// </summary>
        protected const string CoreLoadGremlinQuery = 
            "g.addV('person')" +
                ".property('name', 'marko')" +
                ".property('age', 29)" +
                ".as('marko')" + 
            ".addV('person')" +
                ".property('name', 'vadas')" +
                ".property('age', 27)" +
                ".as('vadas')" +
            ".addV('software')" +
                ".property('name', 'lop')" +
                ".property('lang', 'java')" +
                ".as('lop')" + 
            ".addV('person')" +
                ".property('name', 'josh')" +
                ".property('age', 32)" +
                ".as('josh')" +
            ".addV('software')" +
                ".property('name', 'ripple')" +
                ".property('lang', 'java')" +
                ".as('ripple')" +
            ".addV('person')" +
                ".property('name', 'peter')" +
                ".property('age', 35)" +
                ".as('peter')" +
            ".addE('knows')" +
                ".property('weight', 0.5f)" +
                ".from('marko').to('vadas')" +
            ".addE('knows')" +
                ".property('weight', 1.0f)" +
                ".from('marko').to('josh')" +
            ".addE('created')" +
                ".property('weight', 0.4f)" +
                ".from('marko').to('lop')" +
            ".addE('created')" +
                ".property('weight', 1.0f)" +
                ".from('josh').to('ripple')" +
            ".addE('created')" +
                ".property('weight', 0.4f)" +
                ".from('josh').to('lop')" +
            ".addE('created')" +
                ".property('weight', 0.2f)" +
                ".from('peter').to('lop')";

        /// <summary>
        /// Reference graph: http://www.tinkerpop.com/docs/3.0.0.M1/
        /// </summary>
        protected const string ClassicLoadGremlinQuery = 
            "Vertex marko = graph.addVertex(label, 'person', 'name', 'marko', 'age', 29);\n" +
            "Vertex vadas = graph.addVertex(label, 'person', 'name', 'vadas', 'age', 27);\n" +
            "Vertex lop = graph.addVertex(label, 'software', 'name', 'lop', 'lang', 'java');\n" +
            "Vertex josh = graph.addVertex(label, 'person', 'name', 'josh', 'age', 32);\n" +
            "Vertex ripple = graph.addVertex(label, 'software', 'name', 'ripple', 'lang', 'java');\n" +
            "Vertex peter = graph.addVertex(label, 'person', 'name', 'peter', 'age', 35);\n" +
            "marko.addEdge('knows', vadas, 'weight', 0.5f);\n" +
            "marko.addEdge('knows', josh, 'weight', 1.0f);\n" +
            "marko.addEdge('created', lop, 'weight', 0.4f);\n" +
            "josh.addEdge('created', ripple, 'weight', 1.0f);\n" +
            "josh.addEdge('created', lop, 'weight', 0.4f);\n" +
            "peter.addEdge('created', lop, 'weight', 0.2f);";

        protected const string MakeStrict = "schema.config().option(\"graph.schema_mode\").set(\"production\");";
        protected const string AllowScans = "schema.config().option(\"graph.allow_scan\").set(\"true\");";
        
        /// <summary>
        /// Creates a graph using the current session
        /// </summary>
        public void CreateClassicGraph(ISession session, string name)
        {
            try
            {
                session.ExecuteGraph(new SimpleGraphStatement($"system.graph('{name}').drop()"));
            }
            catch
            {
                // ignored
            }

            session.ExecuteGraph(!TestClusterManager.SupportsNextGenGraph()
                ? new SimpleGraphStatement($"system.graph('{name}').ifNotExists().create()")
                : new SimpleGraphStatement($"system.graph('{name}').ifNotExists().engine(Classic).create()"));
            session.ExecuteGraph(new SimpleGraphStatement(AllowScans).SetGraphName(name));
            session.ExecuteGraph(new SimpleGraphStatement(ClassicSchemaGremlinQuery).SetGraphName(name));
            session.ExecuteGraph(new SimpleGraphStatement(ClassicLoadGremlinQuery).SetGraphName(name));
        }

        /// <summary>
        /// Creates the classic graph using a specific connection
        /// </summary>
        public void CreateClassicGraph(string contactPoint, string name)
        {
            using (var cluster = ClusterBuilder().AddContactPoint(TestClusterManager.InitialContactPoint).Build())
            {
                CreateClassicGraph(cluster.Connect(), name);
            }
        }
        
        /// <summary>
        /// Creates a core graph using the current session
        /// </summary>
        public void CreateCoreGraph(ISession session, string name)
        {
            session.ExecuteGraph(new SimpleGraphStatement($"system.graph('{name}').ifExists().drop()").SetSystemQuery());
            WaitUntilKeyspaceMetadataRefresh(session, name, false);
            session.ExecuteGraph(new SimpleGraphStatement($"system.graph('{name}').ifNotExists().create()").SetSystemQuery());
            WaitUntilKeyspaceMetadataRefresh(session, name, true);
            //session.ExecuteGraph(new SimpleGraphStatement(AllowScans).SetGraphName(name));
            session.ExecuteGraph(new SimpleGraphStatement(CoreSchemaGremlinQuery).SetGraphName(name));
            session.ExecuteGraph(new SimpleGraphStatement(CoreLoadGremlinQuery).SetGraphName(name));
        }

        /// <summary>
        /// Creates the core graph using a specific connection
        /// </summary>
        public void CreateCoreGraph(string contactPoint, string name)
        {
            using (var cluster = ClusterBuilder().AddContactPoint(TestClusterManager.InitialContactPoint).Build())
            {
                CreateCoreGraph(cluster.Connect(), name);
            }
        }

        private void WaitUntilKeyspaceMetadataRefresh(ISession session, string graphName, bool exists)
        {
            var now = DateTime.UtcNow;
            while ((DateTime.UtcNow - now).TotalMilliseconds <= 30000)
            {
                var keyspaceExists = session.Cluster.Metadata.RefreshSchema(graphName);
                if (keyspaceExists == exists)
                {
                    return;
                }

                if (exists)
                {
                    var ks = session.Cluster.Metadata.GetKeyspace(graphName);
                    if (ks?.GraphEngine == "Core")
                    {
                        return;
                    }
                }

                Task.Delay(500).GetAwaiter().GetResult();
            }
            
            Assert.Fail("Keyspace metadata does not have the correct graph engine.");
        }
    }
}
