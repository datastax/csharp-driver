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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cassandra.IntegrationTests
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
