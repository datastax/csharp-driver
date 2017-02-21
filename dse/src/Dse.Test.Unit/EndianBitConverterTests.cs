//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace Dse.Test.Unit
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
