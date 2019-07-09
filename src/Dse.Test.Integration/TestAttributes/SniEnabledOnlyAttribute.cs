// 
//       Copyright (C) DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using System;
using NUnit.Framework;
using NUnit.Framework.Interfaces;

namespace Dse.Test.Integration.TestAttributes
{
    public class SniEnabledOnlyAttribute : NUnitAttribute, IApplyToTest
    {
        public void ApplyToTest(NUnit.Framework.Internal.Test test)
        {
            var envVariable = Environment.GetEnvironmentVariable("SNI_ENABLED");
            if (envVariable == null || !envVariable.Equals("true", StringComparison.OrdinalIgnoreCase))
            {
                test.RunState = RunState.Ignored;
                test.Properties.Set("_SKIPREASON", "Test designed to run with SNI Enabled environments (SNI_ENABLED env variable must be set to TRUE)");
            }
        }
    }
}