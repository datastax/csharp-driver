//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using Dse;
using Dse.Test.Integration.TestClusterManagement;
using Dse.Graph;
using NUnit.Framework;

namespace Dse.Test.Integration
{
    public abstract class BaseIntegrationTest
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
            "schema.edgeLabel('knows').properties('weight').connection('person', 'person').ifNotExists().create();\n" ;

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


        public Version DseVersion
        {
            get { return TestClusterManager.DseVersion; }
        }

        /// <summary>
        /// Creates a graph using the current session
        /// </summary>
        public void CreateClassicGraph(IDseSession session, string name)
        {
            session.ExecuteGraph(new SimpleGraphStatement(string.Format("system.graph('{0}').ifNotExists().create()", name)));
            session.ExecuteGraph(new SimpleGraphStatement(MakeStrict).SetGraphName(name));
            session.ExecuteGraph(new SimpleGraphStatement(AllowScans).SetGraphName(name));
            session.ExecuteGraph(new SimpleGraphStatement(ClassicSchemaGremlinQuery).SetGraphName(name));
            session.ExecuteGraph(new SimpleGraphStatement(ClassicLoadGremlinQuery).SetGraphName(name));
        }

        /// <summary>
        /// Creates the classic graph using a specific connection
        /// </summary>
        public void CreateClassicGraph(string contactPoint, string name)
        {
            using (var cluster = DseCluster.Builder().AddContactPoint(TestClusterManager.InitialContactPoint).Build())
            {
                CreateClassicGraph(cluster.Connect(), name);
            }
        }
    }
}
