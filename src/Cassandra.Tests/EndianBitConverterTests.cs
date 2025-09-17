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
using System;
using System.Linq;
using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;
using CollectionAssert = NUnit.Framework.Legacy.CollectionAssert;

namespace Cassandra.Tests
{
    public class EndianBitConverterTests : BaseUnitTest
    {
        [Test]
        public void ToInt32_SetBytes_Test()
        {
            var values = new[]
            {
                Tuple.Create(0, new byte[] { 0, 0, 0, 0}),
                Tuple.Create(-1, new byte[] { 0xff, 0xff, 0xff, 0xff}),
                Tuple.Create(-2, new byte[] { 0xff, 0xff, 0xff, 0xfe}),
                Tuple.Create(-5661, new byte[] { 0xff, 0xff, 0xe9, 0xe3}),
                Tuple.Create(0x7fffffff, new byte[] { 0x7f, 0xff, 0xff, 0xff})
            };
            foreach (var v in values)
            {
                var buffer = new byte[4];
                EndianBitConverter.SetBytes(false, buffer, 0, v.Item1);
                CollectionAssert.AreEqual(v.Item2, buffer);
                EndianBitConverter.SetBytes(true, buffer, 0, v.Item1);
                CollectionAssert.AreEqual(v.Item2.Reverse(), buffer);
                var decoded = EndianBitConverter.ToInt32(false, v.Item2, 0);
                Assert.AreEqual(v.Item1, decoded);
                decoded = EndianBitConverter.ToInt32(true, v.Item2.Reverse().ToArray(), 0);
                Assert.AreEqual(v.Item1, decoded);
            }
        }

        [Test]
        public void ToDouble_SetBytes_Test()
        {
            var values = new[]
            {
                Tuple.Create(0D, new byte[] { 0, 0, 0, 0, 0, 0, 0, 0}),
                Tuple.Create(-1D, new byte[] { 0xbf, 0xf0, 0, 0, 0, 0, 0, 0}),
                Tuple.Create(-2D, new byte[] { 0xc0, 0, 0, 0, 0, 0, 0, 0}),
                Tuple.Create(-2.3D, new byte[] { 0xc0, 0x02, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66}),
                Tuple.Create(10.3112277778D, new byte[] { 0x40, 0x24, 0x9f, 0x59, 0x3f, 0x4e, 0x83, 0xf8})
            };
            foreach (var v in values)
            {
                var buffer = new byte[8];
                EndianBitConverter.SetBytes(false, buffer, 0, v.Item1);
                CollectionAssert.AreEqual(v.Item2, buffer);
                EndianBitConverter.SetBytes(true, buffer, 0, v.Item1);
                CollectionAssert.AreEqual(v.Item2.Reverse(), buffer);
                var decoded = EndianBitConverter.ToDouble(false, v.Item2, 0);
                Assert.AreEqual(v.Item1, decoded);
                decoded = EndianBitConverter.ToDouble(true, v.Item2.Reverse().ToArray(), 0);
                Assert.AreEqual(v.Item1, decoded);
            }
        }
    }
}
