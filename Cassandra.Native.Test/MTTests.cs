using System;
using System.Collections.Generic;
using System.Text;
using Xunit;
using System.Net;

namespace Cassandra.Native.Test
{
    [Dev.Ignore]
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

    [Dev.Ignore]
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
