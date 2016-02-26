using System.Diagnostics;
using Cassandra;
using Dse.Graph;
using Dse.Test.Integration.ClusterManagement;
using NUnit.Framework;

namespace Dse.Test.Integration
{
    [TestFixture]
    public abstract class BaseIntegrationTest
    {
        protected BaseIntegrationTest()
        {
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Info;
        }
        /// <summary>
        /// Reference graph: http://www.tinkerpop.com/docs/3.0.0.M1/
        /// </summary>
        private const string ClassicSchemaGremlinQuery = "Vertex marko = graph.addVertex('name', 'marko', 'age', 29);" + 
            "Vertex vadas = graph.addVertex('name', 'vadas', 'age', 27);" +
            "Vertex lop = graph.addVertex('name', 'lop', 'lang', 'java');" +
            "Vertex josh = graph.addVertex('name', 'josh', 'age', 32);" +
            "Vertex ripple = graph.addVertex('name', 'ripple', 'lang', 'java');" +
            "Vertex peter = graph.addVertex('name', 'peter', 'age', 35);" +
            "marko.addEdge('knows', vadas, 'weight', 0.5f);" +
            "marko.addEdge('knows', josh, 'weight', 1.0f);" +
            "marko.addEdge('created', lop, 'weight', 0.4f);" +
            "josh.addEdge('created', ripple, 'weight', 1.0f);" +
            "josh.addEdge('created', lop, 'weight', 0.4f);" +
            "peter.addEdge('created', lop, 'weight', 0.2f);";

        /// <summary>
        /// Creates a graph using the current session
        /// </summary>
        public void CreateClassicGraph(IDseSession session, string name)
        {
            session.ExecuteGraph(new SimpleGraphStatement(string.Format("system.createGraph('{0}').ifNotExist().build()", name)));
            session.ExecuteGraph(new SimpleGraphStatement(ClassicSchemaGremlinQuery).SetGraphName(name));
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
