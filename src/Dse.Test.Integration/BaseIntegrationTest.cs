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

        protected const string ClassicSchemaGremlinQuery =
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
            get { return CcmHelper.DseVersion; }
        }

        [OneTimeSetUp]
        public void TestFixtureGlobalSetup()
        {
            VerifyAppropriateDseVersion();
        }

        [SetUp]
        public void IndividualTestSetup()
        {
            VerifyAppropriateDseVersion();
        }

        // If any test is designed for another DSE version, mark it as ignored
        private void VerifyAppropriateDseVersion()
        {
            var test = TestContext.CurrentContext.Test;
            var methodFullName = TestContext.CurrentContext.Test.FullName;
            //var typeName = methodFullName.Substring(0, methodFullName.Length - test.Name.Length - 1);
            var typeName = TestContext.CurrentContext.Test.ClassName;
            var type = Type.GetType(typeName);
            if (type == null)
            {
                return;
            }
            var testName = test.Name;
            if (testName.IndexOf('(') > 0)
            {
                //The test name could be a TestCase: NameOfTheTest(ParameterValue);
                //Remove the parenthesis
                testName = testName.Substring(0, testName.IndexOf('('));
            }
            MethodInfo method = type.GetMethod(testName);
            TestDseVersion methodAttr = null;
            if (method != null)
            {
                methodAttr = method.GetCustomAttribute<TestDseVersion>(true);
            }
            var attr = type.GetTypeInfo().GetCustomAttribute<TestDseVersion>();
            if (attr == null && methodAttr == null)
            {
                //It does not contain the attribute, move on.
                return;
            }
            if (methodAttr != null)
            {
                attr = methodAttr;
            }
            var versionAttr = attr;
            var executingVersion = DseVersion;
            if (!VersionMatch(versionAttr, executingVersion))
                Assert.Ignore(string.Format("Test Ignored: Test suitable to be run against DSE {0}.{1}.{2} {3}", versionAttr.Major, versionAttr.Minor, versionAttr.Build, versionAttr.Comparison >= 0 ? "or above" : "or below"));
        }

        public static bool VersionMatch(TestDseVersion versionAttr, Version executingVersion)
        {
            //Compare them as integers
            var expectedVersion = new Version(versionAttr.Major, versionAttr.Minor, versionAttr.Build);
            var comparison = (Comparison)executingVersion.CompareTo(expectedVersion);

            if (comparison >= Comparison.Equal && versionAttr.Comparison == Comparison.GreaterThanOrEqualsTo)
            {
                return true;
            }
            return comparison == versionAttr.Comparison;
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
            using (var cluster = DseCluster.Builder().AddContactPoint(CcmHelper.InitialContactPoint).Build())
            {
                CreateClassicGraph(cluster.Connect(), name);
            }
        }
    }
}
