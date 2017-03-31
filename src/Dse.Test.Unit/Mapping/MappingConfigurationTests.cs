//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Dse.Mapping;
using Dse.Mapping.TypeConversion;
using Dse.Test.Unit.Mapping.FluentMappings;
using Dse.Test.Unit.Mapping.Pocos;
using NUnit.Framework;

namespace Dse.Test.Unit.Mapping
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
