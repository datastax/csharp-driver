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
        public void CheckDynamicKeySpace_Change()
        {
            var config = new MappingConfiguration();
            config.DefineKeySpaceNameCallBack(GetSpace);
            //Assert.IsNull(config.MapperFactory,"Mapping Factory was null");
            //config.OnKeySpaceRequested += () =>
            //{
            //    return "mydb";
            //};
            Assert.AreSame("mydb", config.MapperFactory.PocoDataFactory.OnKeySpaceRequested?.Invoke());
        }

        public string GetSpace()
        {
            return "mydb";
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
