// 
//       Copyright (C) DataStax Inc.
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

using System;
using Dse.Cloud;
using Dse.Test.Unit.TestAttributes;
using NUnit.Framework;

namespace Dse.Test.Unit.Cloud
{
    [TestFixture]
    public class CloudMetadataServiceTests
    {
        [Test]
        [CloudSupported(Supported = false)]
        public void Should_ThrowNotSupported_When_NetCore20()
        {
            Assert.Throws<NotSupportedException>(() => 
                new CloudMetadataService().GetClusterMetadataAsync(null, null, null).GetAwaiter().GetResult());
        }
    }
}