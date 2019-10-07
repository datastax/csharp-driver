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

using Cassandra.Mapping;
using Cassandra.Mapping.TypeConversion;
using Cassandra.Tests.Mapping.FluentMappings;
using Cassandra.Tests.Mapping.Pocos;
using NUnit.Framework;

namespace Cassandra.Tests.Mapping
{
    [TestFixture]
    public class MappingConfigurationTests
    {
        [Test]
        public void ConvertTypesUsing_Creates_Uses_MapperFactory_Instance()
        {
            var config = new MappingConfiguration();
            var originalMapperFactory = config.MapperFactory;
            //the mapper factory remains the same
            Assert.AreSame(originalMapperFactory, config.MapperFactory);
            config.ConvertTypesUsing(new DefaultTypeConverter());
            //New instance of the mapper factory
            Assert.AreNotSame(originalMapperFactory, config.MapperFactory);
        }

        [Test]
        public void Get_Returns_Mapping_IfExists()
        {
            var userMapping = new FluentUserMapping();
            var mappingConfig = new MappingConfiguration();
            mappingConfig.Define(userMapping);
            var existingMapping = mappingConfig.Get<FluentUser>();
            Assert.IsNotNull(existingMapping);
            Assert.IsInstanceOf(typeof(FluentUserMapping), existingMapping);
        }

        [Test]
        public void Get_Returns_Null_IfDoesNotExist()
        {
            var mappingConfig = new MappingConfiguration();
            var existingMapping = mappingConfig.Get<Album>();
            Assert.IsNull(existingMapping);
        }
    }
}
