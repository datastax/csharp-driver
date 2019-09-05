using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dse.Test.Integration
{
    /// <summary>
    /// Represents a test fixture that on setup, it creates a dse test cluster available for all tests in the fixture.
    /// <para>
    /// It includes the option of a DSE shared session and cluster.
    /// </para>
    /// </summary>
    public abstract class SharedDseClusterTest : SharedClusterTest
    {
        protected new IDseCluster Cluster { get; set; }

        protected new IDseSession Session { get { return (IDseSession) base.Session; } }

        /// <summary>
        /// Gets the builder used to create the shared cluster and session instance
        /// </summary>
        protected virtual DseClusterBuilder BuilderInstance { get { return DseCluster.Builder(); } }

        protected SharedDseClusterTest(int amountOfNodes = 1, bool createSession = true, bool reuse = true)
            : base(amountOfNodes, createSession, reuse)
        {
            
        }

        protected override void CreateCommonSession()
        {
            Cluster = BuilderInstance.AddContactPoint(TestCluster.InitialContactPoint).Build();
            base.Session = Cluster.Connect();
            Session.CreateKeyspace(KeyspaceName);
            Session.ChangeKeyspace(KeyspaceName);
        }
    }
}
