//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Dse.Data.Linq;
using Dse.Test.Integration.Linq.Structures;
using Dse.Test.Integration.TestClusterManagement;
using Dse.Mapping;
using NUnit.Framework;

namespace Dse.Test.Integration.Linq.LinqTable
{
    /// <summary>
    /// NOTE: The GetTable() method is deprecated.
    /// </summary>
    [Category("short")]
    public class GetTable : SharedClusterTest
    {
        string _uniqueKsName;

        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
            Session.CreateKeyspace(_uniqueKsName);
            TestUtils.WaitForSchemaAgreement(Session.Cluster);
            Session.ChangeKeyspace(_uniqueKsName);
        }

        /// <summary>
        /// Get table using GetTable, validate that the resultant table object functions as expected
        /// </summary>
        [Test]
        public void LinqTable_GetTable()
        {
            // Test
            Table<AllDataTypesEntity> table = Session.GetTable<AllDataTypesEntity>();
            table.Create();
            AllDataTypesEntity.WriteReadValidate(table);
        }
    }
}
