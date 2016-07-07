using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Cassandra.IntegrationTests
{
    [AttributeUsage(AttributeTargets.Assembly | AttributeTargets.Class | AttributeTargets.Method, Inherited = false)]
    public class TestTimeoutAttribute : 
#if !NETCORE
        TimeoutAttribute
#else
        Attribute 
#endif
    {
        /// <summary>
        /// Construct a TimeoutAttribute given a time in milliseconds
        /// </summary>
        /// <param name="timeout">The timeout value in milliseconds</param>
        public TestTimeoutAttribute(int timeout)
#if !NETCORE
            : base(timeout)
#endif
        {
            //TODO: Implement an alternate Timeout mechanism for .NET Core
        }
    }
}
