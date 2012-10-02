using System;
using System.Collections.Generic;
using System.Text;
using Xunit;
using System.Net;

namespace Cassandra.Native.Test
{
    public class MTTests : MyUTExt.CommonBasicTests 
    {        
        public MTTests()
            : base(false, true)
        {
        }

        [Fact]
        public void ConnectionsTestBufferedCompressed()
        {
            ConnectionsTest();
        }     
    }
}
