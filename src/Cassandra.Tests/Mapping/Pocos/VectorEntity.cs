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

using Cassandra.Mapping.Attributes;
using System;
using NUnit.Framework;

namespace Cassandra.Tests.Mapping.Pocos
{
    [Table("vectors")]
    public class VectorPoco
    {
        [PartitionKey]
        [Column("uuid_VALUE")]
        public Guid UuidValue { get; set; }

        [ClusteringKey]
        [Column("vector_VALUE")]
        public CqlVector<int> VectorValue { get; set; }

        [Column("vector_of_vectors_VALUE")]
        public CqlVector<CqlVector<int>> VectorOfVectorsValue { get; set; }

        public static VectorPoco Generate(Random r)
        {
            Func<CqlVector<int>> generator = () => new CqlVector<int>(r.Next(), r.Next(), r.Next());
            var vector = generator();
            var vectorOfVectors = new CqlVector<CqlVector<int>>(generator(), generator(), generator());
            return new VectorPoco { UuidValue = Guid.NewGuid(), VectorValue = vector, VectorOfVectorsValue = vectorOfVectors };
        }

        public static void AssertEquals(VectorPoco one, VectorPoco two)
        {
            Assert.AreEqual(one.UuidValue, two.UuidValue);
            CollectionAssert.AreEqual(one.VectorValue, two.VectorValue);
            CollectionAssert.AreEqual(one.VectorOfVectorsValue, two.VectorOfVectorsValue);
        }
    }
}
