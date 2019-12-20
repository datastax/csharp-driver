//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Dse.Data.Linq;
using Dse.Test.Integration.Linq.Structures;
using Dse.Test.Integration.TestClusterManagement;
using Dse.Mapping;
using Dse.Test.Integration.SimulacronAPI.PrimeBuilder.Then;
using NUnit.Framework;

namespace Dse.Test.Integration.Linq.LinqMethods
{
    public class Delete : SimulacronTest
    {
        private List<AllDataTypesEntity> _entityList;
        private readonly string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
        private Table<AllDataTypesEntity> _table;

        [SetUp]
        public override void SetUp()
        {
            base.SetUp();
            _entityList = AllDataTypesEntity.GetDefaultAllDataTypesList();
            _table = new Table<AllDataTypesEntity>(Session, new MappingConfiguration());
        }
        
        [TestCase(true)]
        [TestCase(false)]
        [Test]
        public void Delete_DeleteOneEquals(bool async)
        {
            var table = new Table<AllDataTypesEntity>(Session, new MappingConfiguration());

            AllDataTypesEntity.PrimeCountQuery(TestCluster, _entityList.Count);
            var count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count, count);

            AllDataTypesEntity entityToDelete = _entityList[0];

            var selectQuery = table.Select(m => m).Where(m => m.StringType == entityToDelete.StringType);
            var deleteQuery = selectQuery.Delete();

            if (async)
            {
                deleteQuery.ExecuteAsync().GetAwaiter().GetResult();
            }
            else
            {
                deleteQuery.Execute();
            }

            VerifyBoundStatement(
                $"DELETE FROM \"{AllDataTypesEntity.TableName}\" WHERE \"string_type\" = ?", 1, entityToDelete.StringType);
            
            TestCluster.PrimeDelete();
            AllDataTypesEntity.PrimeCountQuery(TestCluster, _entityList.Count - 1);

            count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count - 1, count);
            TestCluster.PrimeFluent(b => entityToDelete.When(TestCluster, b).ThenVoidSuccess());
            Assert.AreEqual(0, selectQuery.Execute().ToList().Count);
        }
        
        [TestCase(true)]
        [TestCase(false)]
        [Test]
        public void Delete_DeleteMultipleContains(bool async)
        {
            var table = new Table<AllDataTypesEntity>(Session, new MappingConfiguration());

            var uniqueStringKeys = _entityList.Select(m => m.StringType).ToList();
            var deleteRequest = table.Where(m => uniqueStringKeys.Contains(m.StringType)).Delete();
            if (async)
            {
                deleteRequest.ExecuteAsync().GetAwaiter().GetResult();
            }
            else
            {
                deleteRequest.Execute();
            }

            VerifyBoundStatement(
                $"DELETE FROM \"{AllDataTypesEntity.TableName}\" WHERE \"string_type\" IN ?", 1, uniqueStringKeys);
        }

        [Test]
        public void Delete_MissingKey_Sync()
        {
            var table = new Table<AllDataTypesEntity>(Session, new MappingConfiguration());
            
            TestCluster.PrimeFluent(
                b => b.WhenQuery($"DELETE FROM \"{AllDataTypesEntity.TableName}\" WHERE \"boolean_type\" = ?",
                          when => when.WithParam(true))
                      .ThenServerError(ServerError.Invalid, "invalid"));

            var selectQuery = table.Select(m => m).Where(m => m.BooleanType == true);
            var deleteQuery = selectQuery.Delete();

            Assert.Throws<InvalidQueryException>(() => deleteQuery.Execute());
        }

        [Test]
        public void Delete_NoSuchRecord()
        {
            var table = new Table<AllDataTypesEntity>(Session, new MappingConfiguration());

            var entityToDelete = _entityList[0];
            var selectQuery = table.Select(m => m).Where(m => m.StringType == entityToDelete.StringType + Randomm.RandomAlphaNum(16));
            var deleteQuery = selectQuery.Delete();
            deleteQuery.Execute();

            // make sure record was not deleted
            var listOfExecutionParameters = GetBoundStatementExecutionParameters(
                $"DELETE FROM \"{AllDataTypesEntity.TableName}\" WHERE \"string_type\" = ?").ToList();

            Assert.AreEqual(1, listOfExecutionParameters.Count);
            var parameter = Convert.FromBase64String((string)listOfExecutionParameters.Single().Single());
            var actualParameter = (string) Session.Cluster.Metadata.ControlConnection.Serializer.GetCurrentSerializer().Deserialize(parameter, 0, parameter.Length, ColumnTypeCode.Text, null);
            Assert.AreNotEqual(entityToDelete.StringType, actualParameter);
            Assert.IsTrue(actualParameter.StartsWith(entityToDelete.StringType));
            Assert.IsTrue(actualParameter.Length > entityToDelete.StringType.Length);
        }

        /// <summary>
        /// Attempt to delete from a table without specifying a WHERE limiter.  Assert expected failure.
        /// NOTE: Not specifying a 'where' clause in C* is like Delete * in SQL, which is not allowed.
        /// </summary>
        [Test]
        public void Delete_MissingWhereAndSelectClause_Sync()
        {
            TestCluster.PrimeFluent(
                b => b.WhenQuery($"DELETE FROM \"{AllDataTypesEntity.TableName}\"",
                          when => when.WithParam(true))
                      .ThenSyntaxError("invalid"));
            Assert.Throws<SyntaxError>(() => _table.Delete().Execute());
        }

        /// <summary>
        /// Attempt to delete from a table without specifying a WHERE limiter.  Assert expected failure.
        /// NOTE: Not specifying a 'where' clause in C* is like Delete * in SQL, which is not allowed.
        /// </summary>
        [Test]
        public void Delete_MissingWhereClause_Sync()
        {
            TestCluster.PrimeFluent(
                b => b.WhenQuery($"DELETE FROM \"{AllDataTypesEntity.TableName}\"",
                          when => when.WithParam(true))
                      .ThenSyntaxError("invalid"));
            Assert.Throws<SyntaxError>(() => _table.Select(m => m).Delete().Execute());
        }

        /// <summary>
        /// Successfully delete a record using the IfExists condition
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void Delete_IfExists()
        {
            var table = new Table<AllDataTypesEntity>(Session, new MappingConfiguration());
            AllDataTypesEntity entityToDelete = _entityList[0];

            var selectQuery = table.Select(m => m).Where(m => m.StringType == entityToDelete.StringType && m.GuidType == entityToDelete.GuidType);
            var deleteQuery = selectQuery.Delete().IfExists();

            deleteQuery.Execute();

            VerifyBoundStatement(
                $"DELETE FROM \"{AllDataTypesEntity.TableName}\" WHERE \"string_type\" = ? AND \"guid_type\" = ? IF EXISTS", 
                1, 
                entityToDelete.StringType, 
                entityToDelete.GuidType);
        }

        /// <summary>
        /// Successfully delete a record using the IfExists condition, when the row doesn't exist.
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void Delete_IfExists_RowDoesntExist()
        {
            var table = new Table<AllDataTypesEntity>(Session, new MappingConfiguration());
            AllDataTypesEntity entityToDelete = _entityList[0];

            var selectQuery = table.Select(m => m).Where(m => m.StringType == entityToDelete.StringType && m.GuidType == entityToDelete.GuidType);
            var deleteQuery = selectQuery.Delete().IfExists();

            deleteQuery.Execute();
            
            VerifyBoundStatement(
                $"DELETE FROM \"{AllDataTypesEntity.TableName}\" WHERE \"string_type\" = ? AND \"guid_type\" = ? IF EXISTS", 
                1, 
                entityToDelete.StringType, 
                entityToDelete.GuidType);

            // Executing again should not fail, should just be a no-op
            deleteQuery.Execute();
            
            VerifyBoundStatement(
                $"DELETE FROM \"{AllDataTypesEntity.TableName}\" WHERE \"string_type\" = ? AND \"guid_type\" = ? IF EXISTS", 
                2, 
                entityToDelete.StringType, 
                entityToDelete.GuidType);
        }

        /// <summary>
        ///
        /// </summary>
        [Test, TestCassandraVersion(2, 1, 2)]
        public void Delete_IfExists_ClusteringKeyOmitted()
        {
            var table = new Table<AllDataTypesEntity>(Session, new MappingConfiguration());
            AllDataTypesEntity entityToDelete = _entityList[0];
            
            TestCluster.PrimeFluent(
                b => b.WhenQuery($"DELETE FROM \"{AllDataTypesEntity.TableName}\" WHERE \"string_type\" = ? IF EXISTS",
                          when => when.WithParam(entityToDelete.StringType))
                      .ThenServerError(ServerError.Invalid, "invalid"));

            var selectQuery = table.Select(m => m).Where(m => m.StringType == entityToDelete.StringType);
            var deleteQuery = selectQuery.Delete().IfExists();

            Assert.Throws<InvalidQueryException>(() => deleteQuery.Execute());
        }

        [TestCase(BatchType.Unlogged)]
        [TestCase(BatchType.Logged)]
        [TestCase(default(BatchType))]
        [TestCassandraVersion(2, 0)]
        public void Delete_BatchType(BatchType batchType)
        {
            var table = new Table<AllDataTypesEntity>(Session, new MappingConfiguration());
            AllDataTypesEntity entityToDelete = _entityList[0];
            AllDataTypesEntity entityToDelete2 = _entityList[1];

            var selectQuery = table.Select(m => m).Where(m => m.StringType == entityToDelete.StringType);
            var deleteQuery = selectQuery.Delete();
            var selectQuery2 = table.Select(m => m).Where(m => m.StringType == entityToDelete2.StringType);
            var deleteQuery2 = selectQuery2.Delete();

            var batch = table.GetSession().CreateBatch(batchType);
            batch.Append(deleteQuery);
            batch.Append(deleteQuery2);
            batch.Execute();

            VerifyBatchStatement(
                1, 
                new [] 
                {
                    $"DELETE FROM \"{AllDataTypesEntity.TableName}\" WHERE \"string_type\" = ?",
                    $"DELETE FROM \"{AllDataTypesEntity.TableName}\" WHERE \"string_type\" = ?"
                },
                new[] { new object [] { entityToDelete.StringType }, new object [] { entityToDelete2.StringType }});
        }
    }
}