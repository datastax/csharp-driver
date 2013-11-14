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
﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;

#if MYTEST
using MyTest;
using System.Threading;
#else
using Microsoft.VisualStudio.TestTools.UnitTesting;
#endif


namespace Cassandra.MSTest
{

    [TestClass]
    public partial class PreparedStatementsCCMTests
    {     
        Session Session;


        public PreparedStatementsCCMTests()
        {
        }

        [TestInitialize]
        public void SetFixture()
        {
            CCMBridge.ReusableCCMCluster.Setup(2);
            CCMBridge.ReusableCCMCluster.Build(Cluster.Builder());
            Session = CCMBridge.ReusableCCMCluster.Connect();
        }

        [TestCleanup]
        public void Dispose()
        {
            CCMBridge.ReusableCCMCluster.Drop();
        }

        private void reprepareOnNewlyUpNodeTest(bool useKeyspace)
        {
            string keyspace = "tester";
            Session.CreateKeyspaceIfNotExists(keyspace);
            string modifiedKs = "";

            if (useKeyspace)
                Session.ChangeKeyspace(keyspace);
            else
                modifiedKs = keyspace + ".";

            try
            {
                Session.WaitForSchemaAgreement(
                    Session.Execute("CREATE TABLE " + modifiedKs + "test(k text PRIMARY KEY, i int)")
                );
            }
            catch (AlreadyExistsException)
            {
            }
            Session.Execute("INSERT INTO " + modifiedKs +"test (k, i) VALUES ('123', 17)");
            Session.Execute("INSERT INTO " + modifiedKs +"test (k, i) VALUES ('124', 18)");

            PreparedStatement ps = Session.Prepare("SELECT * FROM " + modifiedKs + "test WHERE k = ?");

            using (var rs = Session.Execute(ps.Bind("123")))
            {
                Assert.Equal(rs.GetRows().First().GetValue<int>("i"), 17); // ERROR
            }
            CCMBridge.ReusableCCMCluster.CCMBridge.Stop();            
            TestUtils.waitForDown(Options.Default.IP_PREFIX + "1", Session.Cluster, 30);

            CCMBridge.ReusableCCMCluster.CCMBridge.Start();
            TestUtils.waitFor(Options.Default.IP_PREFIX + "1", Session.Cluster, 30);

            try
            {
                using (var rowset = Session.Execute(ps.Bind("124")))
                {
                    Assert.Equal(rowset.GetRows().First().GetValue<int>("i"), 18);
                }
            }
            catch (NoHostAvailableException e)
            {
                Debug.WriteLine(">> " + e.Errors);
                throw e;
            }
        }
    }
}
