//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using Dse.Data;
using NUnit.Framework;

namespace Dse.Test.Unit
{
    [TestFixture]
    public class CqlParameterCollectionTest
    {
        [Test]
        public void TestCqlParameterCollection()
        {
            var target = new CqlParameterCollection();

            // test Count
            Assert.AreEqual(0, target.Count);
            var p1 = target.Add("p1", 1);
            Assert.AreEqual(1, target.Count);

            // test SyncRoot
            Assert.IsNotNull(target.SyncRoot);
            Assert.AreEqual(target.SyncRoot, target.SyncRoot);

#if !NETCORE
            // test IsFixedSize
            Assert.IsFalse(target.IsFixedSize);

            // test IsReadOnly
            Assert.IsFalse(target.IsReadOnly);

            // test IsSynchronized
            Assert.IsFalse(target.IsSynchronized);
#endif
            // test Add()
            var p2Index = target.Add(new CqlParameter("p2"));
            Assert.AreEqual(2, target.Count);
            Assert.AreEqual(1, p2Index);

            // test Contains()
            var p3 = new CqlParameter("p3");
            Assert.IsTrue(target.Contains(p1));
            Assert.IsFalse(target.Contains(p3));

            // test IndexOf()
            Assert.AreEqual(0, target.IndexOf(p1));

            // test Insert();
            target.Insert(0, p3);
            Assert.AreEqual(0, target.IndexOf(p3));
            Assert.AreEqual(1, target.IndexOf(p1));

            // test Remove()
            var toBeRemove = new CqlParameter("toberemoved");
            target.Add(toBeRemove);
            Assert.IsTrue(target.Contains(toBeRemove));
            target.Remove(toBeRemove);
            Assert.IsFalse(target.Contains(toBeRemove));

            // test RemoveAt()
            target.RemoveAt(0);
            Assert.AreEqual(2, target.Count);
            target.RemoveAt("p2");
            Assert.IsFalse(target.Contains("p2"));

            // test CopyTo()
            var arr = new CqlParameter[1];
            target.CopyTo(arr, 0);
            Assert.AreEqual(arr[0], target[0]);

            // test AddRange()
            var p4p5 = new[] { new CqlParameter("p4"), new CqlParameter("p5") };
            target.AddRange(p4p5);
            Assert.AreEqual(3, target.Count);
            Assert.IsTrue(target.Contains(p4p5[0]));
            Assert.IsTrue(target.Contains(p4p5[1]));

            // test Clear()
            target.Clear();
            Assert.AreEqual(0, target.Count);
        }
    }

}
