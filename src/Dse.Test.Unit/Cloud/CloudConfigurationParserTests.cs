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
using System.IO;
using System.Text;
using Dse.Cloud;
using NUnit.Framework;

namespace Dse.Test.Unit.Cloud
{
    [TestFixture]
    public class CloudConfigurationParserTests
    {
        [Test]
        public void Should_ParseCorrectly_When_AllPropertiesAreThereAndMore()
        {
            var a = 
@"{
   ""username"": ""joaoreis"",
   ""password"": ""joaoreis123"",
   ""host"": ""ea20d9d5-f69e-46ad-8eb1-f32f33a7472e.us-east-1.dse.datastax.com"",
   ""port"": 30443,
   ""keyspace"": ""joaoreisks"",
   ""localDC"": ""aws-us-east-1"",
   ""caCertLocation"": ""./ca.crt"",
   ""keyLocation"": ""./key"",
   ""certLocation"": ""./cert"",
   ""keyStoreLocation"": ""./identity.jks"",
   ""keyStorePassword"": ""2VuA5qDGkPt7xS1B8"",
   ""trustStoreLocation"": ""./trustStore.jks"",
   ""trustStorePassword"": ""X4V0xOzGu1Tsy6JZ3"",
   ""csvLocation"": ""./data""
}";
            var stream = new MemoryStream(Encoding.Default.GetBytes(a));
            var target = new CloudConfigurationParser();
            
            var config = target.ParseConfig(stream);
            
            Assert.AreEqual(30443, config.Port);
            Assert.AreEqual("joaoreis123", config.Password);
            Assert.AreEqual("joaoreis", config.Username);
            Assert.AreEqual("ea20d9d5-f69e-46ad-8eb1-f32f33a7472e.us-east-1.dse.datastax.com", config.Host);
        }
        
        [Test]
        public void Should_ParseCorrectly_When_OnlyRequiredPropertiesAreThere()
        {
            var a = 
@"{
   ""host"": ""ea20d9d5-f69e-46ad-8eb1-f32f33a7472e.us-east-1.dse.datastax.com"",
   ""port"": 30443,
   ""localDC"": ""aws-us-east-1""
}";
            var stream = new MemoryStream(Encoding.Default.GetBytes(a));
            var target = new CloudConfigurationParser();
            
            var config = target.ParseConfig(stream);
            
            Assert.AreEqual(30443, config.Port);
            Assert.IsNull(config.Password);
            Assert.IsNull(config.Username);
            Assert.AreEqual("ea20d9d5-f69e-46ad-8eb1-f32f33a7472e.us-east-1.dse.datastax.com", config.Host);
        }
        
        [Test]
        public void Should_ParseCorrectly_When_AllPropertiesAreThere()
        {
            var a = 
@"{
   ""username"": ""joaoreis"",
   ""password"": ""joaoreis123"",
   ""host"": ""ea20d9d5-f69e-46ad-8eb1-f32f33a7472e.us-east-1.dse.datastax.com"",
   ""port"": 30443,
   ""keyspace"": ""joaoreisks"",
   ""localDC"": ""aws-us-east-1""
}";
            var stream = new MemoryStream(Encoding.Default.GetBytes(a));
            var target = new CloudConfigurationParser();
            
            var config = target.ParseConfig(stream);
            
            Assert.AreEqual(30443, config.Port);
            Assert.AreEqual("joaoreis123", config.Password);
            Assert.AreEqual("joaoreis", config.Username);
            Assert.AreEqual("ea20d9d5-f69e-46ad-8eb1-f32f33a7472e.us-east-1.dse.datastax.com", config.Host);
        }
        
        [Test]
        public void Should_ThrowException_When_HostIsNotThere()
        {
            var a = 
@"{
   ""username"": ""joaoreis"",
   ""password"": ""joaoreis123"",
   ""port"": 30443,
   ""keyspace"": ""joaoreisks"",
   ""localDC"": ""aws-us-east-1""
}";
            var stream = new MemoryStream(Encoding.Default.GetBytes(a));
            var target = new CloudConfigurationParser();
            
            var ex = Assert.Throws<ArgumentException>(() => target.ParseConfig(stream));
            
            Assert.IsTrue(ex.Message.Contains("Could not parse the \"host\""), ex.Message);
        }
        
        [Test]
        public void Should_ThrowException_When_PortIsNotThere()
        {
            var a = 
@"{
   ""username"": ""joaoreis"",
   ""password"": ""joaoreis123"",
   ""host"": ""ea20d9d5-f69e-46ad-8eb1-f32f33a7472e.us-east-1.dse.datastax.com"",
   ""keyspace"": ""joaoreisks"",
   ""localDC"": ""aws-us-east-1""
}";
            var stream = new MemoryStream(Encoding.Default.GetBytes(a));
            var target = new CloudConfigurationParser();
            
            var ex = Assert.Throws<ArgumentException>(() => target.ParseConfig(stream));
            
            Assert.IsTrue(ex.Message.Contains("Could not parse the \"port\""), ex.Message);
        }
        
        [Test]
        public void Should_ThrowException_When_ConfigIsEmpty()
        {
            var a = @"";
            var stream = new MemoryStream(Encoding.Default.GetBytes(a));
            var target = new CloudConfigurationParser();
            
            var ex = Assert.Throws<ArgumentException>(() => target.ParseConfig(stream));
            
            Assert.IsTrue(ex.Message.Contains("Config file is empty"), ex.Message);
        }
        
        [Test]
        public void Should_ThrowException_When_ConfigDoesntHaveAnyRelevantProperty()
        {
            var a = @"{""keyStoreLocation"": ""./identity.jks""}";
            var stream = new MemoryStream(Encoding.Default.GetBytes(a));
            var target = new CloudConfigurationParser();
            
            var ex = Assert.Throws<ArgumentException>(() => target.ParseConfig(stream));
            
            Assert.IsTrue(ex.Message.Contains("Could not parse"), ex.Message);
        }
    }
}