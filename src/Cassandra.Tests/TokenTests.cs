using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    public class TokenTests
    {
        [Test]
        [Explicit]
        public void MurmurHashTest()
        {
            //inputs and result values from Cassandra
            var values = new Dictionary<byte[], M3PToken>()
            {
                {new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},                             new M3PToken(-5563837382979743776L)},
                {new byte[] {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17},                            new M3PToken(-1513403162740402161L)},
                {new byte[] {3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18},                           new M3PToken(-495360443712684655L)},
                {new byte[] {4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19},                          new M3PToken(1734091135765407943L)},
                {new byte[] {5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},                         new M3PToken(-3199412112042527988L)},
                {new byte[] {6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21},                        new M3PToken(-6316563938475080831L)},
                {new byte[] {7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22},                       new M3PToken(8228893370679682632L)},
                {new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},                                    new M3PToken(5457549051747178710L)},
                {new byte[] {255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},    new M3PToken(-2824192546314762522L)},
                {new byte[] {254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254},    new M3PToken(-833317529301936754)},
                {new byte[] {000, 001, 002, 003, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},    new M3PToken(6463632673159404390L)},
                {new byte[] {254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254},    new M3PToken(-1672437813826982685L)},
                {new byte[] {254, 254, 254, 254},    new M3PToken(4566408979886474012L)},
                {new byte[] {0, 0, 0, 0},    new M3PToken(-3485513579396041028L)},
                {new byte[] {0, 1, 127, 127},    new M3PToken(6573459401642635627)},
                {new byte[] {0, 255, 255, 255},    new M3PToken(123573637386978882)},
                {new byte[] {255, 1, 2, 3},    new M3PToken(-2839127690952877842)},
                {new byte[] {000, 001, 002, 003, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},    new M3PToken(6463632673159404390L)},
            };
            var factory = new M3PToken.M3PTokenFactory();
            foreach (var kv in values)
            {
                Assert.AreEqual(kv.Value, factory.Hash(kv.Key));
            }
        }
    }
}
