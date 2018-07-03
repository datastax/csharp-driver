// 
//       Copyright DataStax Inc.
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

using System.Linq;
using System.Net;
using Cassandra.Tasks;
using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    public class AddressTranslatorTests
    {
        [Test]
        public void EC2MultiRegionTranslator_Should_Return_The_Same_Address_When_Resolution_Fails()
        {
            var translator = new EC2MultiRegionTranslator();
            // Use TEST-NET-1: https://tools.ietf.org/html/rfc5735
            var address = new IPEndPoint(IPAddress.Parse("192.0.2.1"), 9042);
            Assert.AreEqual(address, translator.Translate(address));
        }

        [Test]
        public void EC2MultiRegionTranslator_Should_Do_Reverse_And_A_Forward_Dns_Lookup()
        {
            var ip = TaskHelper.WaitToComplete(Dns.GetHostAddressesAsync("datastax.com")).First();
            var translator = new EC2MultiRegionTranslator();
            var address = new IPEndPoint(ip, 8888);
            Assert.AreEqual(address, translator.Translate(address));
        }
    }
}