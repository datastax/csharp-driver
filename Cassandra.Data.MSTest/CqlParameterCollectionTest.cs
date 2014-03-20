//
//      Copyright (C) 2012 DataStax Inc.
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
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cassandra.Data.MSTest
{
    [TestClass]
    public class CqlParameterCollectionTest
    {
        [TestMethod]
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

            // test IsFixedSize
            Assert.IsFalse(target.IsFixedSize);

            // test IsReadOnly
            Assert.IsFalse(target.IsReadOnly);

            // test IsSynchronized
            Assert.IsFalse(target.IsSynchronized);

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
