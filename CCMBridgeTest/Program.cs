using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CCMBridgeTest
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var cluster = Cassandra.CCMBridge.Create("test", 3))
            {
                cluster.Stop();
                cluster.Start();
            }
        }
    }
}
