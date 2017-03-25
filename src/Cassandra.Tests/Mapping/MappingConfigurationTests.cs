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
        public void Get_FromGlobalConfig_Returns_Mapping_IfExists()
        {
            var userMapping = new FluentUserMapping();
            MappingConfiguration.Global.Define(userMapping);
            var existingMapping = MappingConfiguration.Global.Get<FluentUser>();
            Assert.IsNotNull(existingMapping);
            Assert.IsInstanceOf(typeof(FluentUserMapping), existingMapping);
        }

        [Test]
        public void Get_FromGlobalConfig_Returns_Null_IfDoesNotExist()
        {
            var existingMapping = MappingConfiguration.Global.Get<Album>();
            Assert.IsNull(existingMapping);
        }
    }
}
