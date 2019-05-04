// 
//       Copyright (C) 2019 DataStax Inc.
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
// 

using Dse.ExecutionProfiles;
using Dse.Graph;
using NUnit.Framework;

namespace Dse.Test.Unit.ExecutionProfiles
{
    [TestFixture]
    public class DseExecutionProfileBuilderTests
    {
        [Test]
        public void Should_GetAllSettingsFromBaseProfile_When_DerivedProfileHasNoSettings()
        { 
            var go = new GraphOptions();
            var baseProfile = new ExecutionProfileBuilder()
                                              .WithGraphOptions(go)
                                              .Build();

            var profile = new ExecutionProfile(baseProfile, new ExecutionProfileBuilder().Build());

            Assert.AreEqual(go, profile.GraphOptions);
        }
        
        [Test]
        public void Should_GetNoSettingFromBaseProfile_When_DerivedProfileHasAllSettings()
        { 
            var go = new GraphOptions().SetName("ee");
            var goProfile = new GraphOptions().SetName("tt");
            var baseProfile = new ExecutionProfileBuilder()
                                              .WithGraphOptions(go)
                                              .Build();

            
            var derivedProfile = new ExecutionProfileBuilder()
                                          .WithGraphOptions(goProfile)
                                          .Build();
            
            var profile = new ExecutionProfile(baseProfile, derivedProfile);

            Assert.AreSame(goProfile, profile.GraphOptions);
        }
    }
}