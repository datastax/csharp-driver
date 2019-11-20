//
//      Copyright (C) DataStax Inc.
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

using System;
using NUnit.Framework;
using NUnit.Framework.Interfaces;

namespace Cassandra.IntegrationTests.TestAttributes
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