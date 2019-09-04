//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using Dse.Data.Linq;
using Dse.Test.Integration.CqlFunctions.Structures;
using Dse.Test.Integration.TestClusterManagement;
using Dse.Mapping;
using NUnit.Framework;
#pragma warning disable 612

namespace Dse.Test.Integration.CqlFunctions.Tests
{
    [Category("short"), Category("realcluster")]
    public class Token : SharedClusterTest
    {
        private ISession _session = null;
        private List<EntityWithTimeUuid> _expectedTimeUuidObjectList;
        private readonly string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
        private Table<EntityWithTimeUuid> _tableEntityWithTimeUuid;
        private Table<EntityWithNullableTimeUuid> _tableEntityWithNullableTimeUuid;

        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            _session = Session;
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);

            // Create necessary tables
            MappingConfiguration config1 = new MappingConfiguration();
            config1.MapperFactory.PocoDataFactory.AddDefinitionDefault(typeof (EntityWithTimeUuid),
                () => LinqAttributeBasedTypeDefinition.DetermineAttributes(typeof (EntityWithTimeUuid)));
            _tableEntityWithTimeUuid = new Table<EntityWithTimeUuid>(_session, config1);
            _tableEntityWithTimeUuid.Create();

            MappingConfiguration config2 = new MappingConfiguration();
            config2.MapperFactory.PocoDataFactory.AddDefinitionDefault(typeof (EntityWithNullableTimeUuid),
                () => LinqAttributeBasedTypeDefinition.DetermineAttributes(typeof (EntityWithNullableTimeUuid)));
            _tableEntityWithNullableTimeUuid = new Table<EntityWithNullableTimeUuid>(_session, config2);
            _tableEntityWithNullableTimeUuid.Create();

            _expectedTimeUuidObjectList = EntityWithTimeUuid.GetDefaultObjectList();
            for (int i=0; i<_expectedTimeUuidObjectList.Count; i++)
            {
                _expectedTimeUuidObjectList[i].StringType = i.ToString();
            }
        }

        /// <summary>
        /// Validate that the LinqUtility function Token, which corresponds to the CQL query token
        /// functions as expected when using a 'equals' comparison, comparing string values
        /// </summary>
        [Test]
        public void Token_EqualTo_String()
        {
            EntityWithTimeUuid.SetupEntity(_tableEntityWithTimeUuid, _expectedTimeUuidObjectList);
            foreach (EntityWithTimeUuid singleEntity in _expectedTimeUuidObjectList)
            {
                var whereQuery = _tableEntityWithTimeUuid.Where(s => CqlFunction.Token(s.StringType) == CqlFunction.Token(singleEntity.StringType));
                List<EntityWithTimeUuid> objectsReturned1 = whereQuery.ExecuteAsync().Result.ToList();
                Assert.AreEqual(1, objectsReturned1.Count);
                EntityWithTimeUuid.AssertEquals(singleEntity, objectsReturned1.First());

                foreach (var actualObj in objectsReturned1)
                    EntityWithTimeUuid.AssertListContains(_expectedTimeUuidObjectList, actualObj);
            }

            // change to query that returns nothing 
            var whereQueryReturnsNothing =_tableEntityWithTimeUuid.Where(s => CqlFunction.Token(s.StringType) == CqlFunction.Token(Guid.NewGuid().ToString()));
            List<EntityWithTimeUuid> objectsReturned2 = whereQueryReturnsNothing.ExecuteAsync().Result.ToList();
            Assert.AreEqual(0, objectsReturned2.Count);
        }

        /// <summary>
        /// Validate that the LinqUtility function Token, which corresponds to the CQL query token
        /// functions as expected when using a 'less than' comparison, comparing string values
        /// </summary>
        [Test]
        public void Token_LessThan_String()
        {
            EntityWithTimeUuid.SetupEntity(_tableEntityWithTimeUuid, _expectedTimeUuidObjectList);
            List<EntityWithTimeUuid> listAsTheyAreInCassandra = _tableEntityWithTimeUuid.Execute().ToList();
            Assert.AreEqual(_expectedTimeUuidObjectList.Count, listAsTheyAreInCassandra.Count);
            for (int i = 0; i < listAsTheyAreInCassandra.Count; i++)
            {
                EntityWithTimeUuid singleEntity = listAsTheyAreInCassandra[i];
                var whereQuery = _tableEntityWithTimeUuid.Where(s => CqlFunction.Token(s.StringType) < CqlFunction.Token(singleEntity.StringType));
                List<EntityWithTimeUuid> objectsReturned1 = whereQuery.ExecuteAsync().Result.ToList();
                Assert.AreEqual(i, objectsReturned1.Count);

                foreach (var actualObj in objectsReturned1)
                    EntityWithTimeUuid.AssertListContains(_expectedTimeUuidObjectList, actualObj);
            }

        }

        /// <summary>
        /// Validate that the LinqUtility function Token, which corresponds to the CQL query token
        /// functions as expected when using a 'greater than' comparison, comparing string values
        /// </summary>
        [Test]
        public void Token_GreaterThan_String()
        {
            EntityWithTimeUuid.SetupEntity(_tableEntityWithTimeUuid, _expectedTimeUuidObjectList);
            List<EntityWithTimeUuid> listAsTheyAreInCassandra = _tableEntityWithTimeUuid.Execute().ToList();
            Assert.AreEqual(_expectedTimeUuidObjectList.Count, listAsTheyAreInCassandra.Count);
            int independentInterator = 5;
            for (int i = 0; i < listAsTheyAreInCassandra.Count; i++)
            {
                EntityWithTimeUuid singleEntity = listAsTheyAreInCassandra[i];
                var whereQuery = _tableEntityWithTimeUuid.Where(s => CqlFunction.Token(s.StringType) > CqlFunction.Token(singleEntity.StringType));
                List<EntityWithTimeUuid> objectsReturned1 = whereQuery.ExecuteAsync().Result.ToList();
                Assert.AreEqual(independentInterator, objectsReturned1.Count);

                foreach (var actualObj in objectsReturned1)
                    EntityWithTimeUuid.AssertListContains(_expectedTimeUuidObjectList, actualObj);

                independentInterator--;
            }

        }

    }



}
