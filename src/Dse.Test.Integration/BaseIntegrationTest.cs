using System.Diagnostics;
using Cassandra;
using Dse.Graph;
using Dse.Test.Integration.ClusterManagement;
using NUnit.Framework;

namespace Dse.Test.Integration
{
    [TestFixture, Category("short")]
    public abstract class BaseIntegrationTest
    {
        protected BaseIntegrationTest()
        {
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Info;
        }

        private const string ClassicSchemaGremlinQuery = 
            "schema.propertyKey('name').Text().ifNotExists().create();\n" +
            "schema.propertyKey('age').Int().ifNotExists().create();\n" +
            "schema.propertyKey('lang').Text().ifNotExists().create();\n" +
            "schema.propertyKey('weight').Float().ifNotExists().create();\n" +
            "schema.vertexLabel('person').ifNotExists().create();\n" +
            "schema.vertexLabel('person').properties('name', 'age').add();\n" +
            "schema.vertexLabel('software').ifNotExists().create();\n" +
            "schema.vertexLabel('software').properties('name', 'lang').add();\n" +
            "schema.edgeLabel('created').ifNotExists().create();\n" +
            "schema.edgeLabel('created').properties('weight').add();\n" +
            "schema.edgeLabel('created').connection('person', 'software').add();\n" +
            "schema.edgeLabel('created').connection('software', 'software').add();\n" +
            "schema.edgeLabel('knows').ifNotExists().create();\n" +
            "schema.edgeLabel('knows').properties('weight').add();\n" +
            "schema.edgeLabel('knows').connection('person', 'person').add();";

        /// <summary>
        /// Reference graph: http://www.tinkerpop.com/docs/3.0.0.M1/
        /// </summary>
        private const string ClassicLoadGremlinQuery = 
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

        /// <summary>
        /// Creates a graph using the current session
        /// </summary>
        public void CreateClassicGraph(IDseSession session, string name)
        {
            session.ExecuteGraph(new SimpleGraphStatement(string.Format("system.graph('{0}').ifNotExists().create()", name)));
            session.ExecuteGraph(new SimpleGraphStatement(ClassicSchemaGremlinQuery).SetGraphName(name));
            session.ExecuteGraph(new SimpleGraphStatement(ClassicLoadGremlinQuery).SetGraphName(name));
        }

        /// <summary>
        /// Creates the classic graph using a specific connection
        /// </summary>
        public void CreateClassicGraph(string contactPoint, string name)
        {
            using (var cluster = DseCluster.Builder().AddContactPoint(CcmHelper.InitialContactPoint).Build())
            {
                CreateClassicGraph(cluster.Connect(), name);
            }
        }
    }
}
