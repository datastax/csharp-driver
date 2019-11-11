//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using NUnit.Framework;

namespace Dse.Test.Unit
{
    [AttributeUsage(AttributeTargets.Assembly | AttributeTargets.Class | AttributeTargets.Method, Inherited = false)]
    public class TestTimeoutAttribute : 
#if NETFRAMEWORK
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
#if NETFRAMEWORK
            : base(timeout)
#endif
        {
            //TODO: Implement an alternate Timeout mechanism for .NET Core
        }
    }
}
