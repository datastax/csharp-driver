using System;
using NUnit.Framework;

namespace Cassandra.Tests
{
    [AttributeUsage(AttributeTargets.Assembly | AttributeTargets.Class | AttributeTargets.Method, Inherited = false)]
    public class TestTimeoutAttribute : 
#if NET452
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
#if NET452
            : base(timeout)
#endif
        {
            //TODO: Implement an alternate Timeout mechanism for .NET Core
        }
    }
}
