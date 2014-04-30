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
using System.Numerics;

#if MYTEST
using MyTest;
#else
using Microsoft.VisualStudio.TestTools.UnitTesting;
#endif

namespace Cassandra.MSTest
{
    public partial class PreparedStatementsTests
    {        
        [TestMethod]
        [WorksForMe]
        public void testMassivePrepared()
        {
            massivePreparedStatementTest();
        }

        [TestMethod]
		[WorksForMe]
        public void testPreparedDecimal()
        {
            insertingSingleValuePrepared(typeof(System.Decimal));
        }

        [TestMethod] 
		[WorksForMe]
        public void testPreparedVarInt()
        {
            insertingSingleValuePrepared(typeof(BigInteger));            
        }

        [TestMethod]
		[WorksForMe]
        public void testPreparedBigInt()
        {
            insertingSingleValuePrepared(typeof(System.Int64));
        }

        [TestMethod]
		[WorksForMe]
        public void testPreparedDouble()
        {
            insertingSingleValuePrepared(typeof(System.Double));
        }

        [TestMethod]
		[WorksForMe]
        public void testPreparedFloat()
        {
            insertingSingleValuePrepared(typeof(System.Single));
        }

        [TestMethod]
		[WorksForMe]
        public void testPreparedInt()
        {
            insertingSingleValuePrepared(typeof(System.Int32));
        }

        [TestMethod]
		[WorksForMe]
        public void testPreparedVarchar()
        {
            insertingSingleValuePrepared(typeof(System.String));
        }

        [TestMethod]
		[WorksForMe]
        public void testPreparedTimestamp()
        {
            insertingSingleValuePrepared(typeof(System.DateTimeOffset));
        }

        [TestMethod]
        [WorksForMe]
        public void testPreparedBoolean()
        {
            insertingSingleValuePrepared(typeof(System.Boolean));
        }

        [TestMethod]
        [WorksForMe]
        public void testPreparedBlob()
        {
            insertingSingleValuePrepared(typeof(System.Byte));
        }

        [TestMethod]
        [WorksForMe]
        public void testPreparedInet()
        {
            insertingSingleValuePrepared(typeof(System.Net.IPAddress));
        }
        
        [TestMethod]   
		[WorksForMe]
        public void testPreparedUUID()
        {
            insertingSingleValuePrepared(typeof(System.Guid));
        }                        
    }
}