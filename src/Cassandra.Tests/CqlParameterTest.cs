//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Data;
using Cassandra.Data;
using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    public class CqlParameterTest
    {
        [Test]
        public void TestCqlParameter()
        {
            var name = "p1";
            var value = 1;
            var target = new CqlParameter(name, value);

            // test ParameterName
            var formattedName = ":p1";
            var name2 = ":p2";
            Assert.AreEqual(formattedName, target.ParameterName);
            target.ParameterName = name2;
            Assert.AreEqual(name2, target.ParameterName);

            // test IsNullable & SourceColumnNullMapping
            Assert.IsTrue(target.IsNullable);
            Assert.IsTrue(target.SourceColumnNullMapping);
            target.IsNullable = false;
            Assert.IsFalse(target.IsNullable);
            Assert.IsFalse(target.SourceColumnNullMapping);

            // test Direction, only Input is supported
            Assert.AreEqual(ParameterDirection.Input, target.Direction);
            Exception ex = null;
            try
            {
                target.Direction = ParameterDirection.Output;
            }
            catch (Exception e)
            {
                ex = e;
            }
            Assert.IsNotNull(ex);

            // test Value
            Assert.AreEqual(value, target.Value);
            var value2 = "2";
            target.Value = value2;
            Assert.AreEqual(value2, target.Value);

            // test Size, it should always return 0
            Assert.AreEqual(0, target.Size);
            target.Size = 1;
            Assert.AreEqual(0, target.Size);


        }
    }

}
