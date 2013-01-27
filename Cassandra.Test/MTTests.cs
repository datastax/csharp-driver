using System;
using System.Collections.Generic;
using System.Text;
using Dev;
using System.Net;

namespace Cassandra.Test
{
    public class BigInsertCompressedTests : MyUTExt.CommonBasicTests 
    {        
        public BigInsertCompressedTests()
            : base(true) 
        {
        }

        [Fact]
        public void RunTest()
        {
            Test(3000);
        }     
    }

    public class BigInsertNoCompressoionTests : MyUTExt.CommonBasicTests
    {
        public BigInsertNoCompressoionTests()
            : base(false)
        {
        }

        [Fact]
        public void RunTest()
        {
            Test(3000);
        }
    }
}
