/*
 * Copyright (C) 2012 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using Xunit;

namespace Snappy.Test
{
    public class SnappyTests
    {
        [Fact]
        public void CompressRandomData()
        {
            var r = new Random();
            var rng = new RNGCryptoServiceProvider();
            for (int i = 0; i < 10000; i++)
            {
                var data = new byte[r.Next(UInt16.MaxValue)];
                rng.GetNonZeroBytes(data);
                var compressedData = Snappy.Compress(data);
                var decompressedData = Snappy.Decompress(compressedData, 0, compressedData.Length);
                Assert.True(decompressedData.SequenceEqual(data));
            }
        }

        [Fact]
        public void CompressFiles()
        {
            var files = Directory.EnumerateFiles(@"..\..\..\Data");
            foreach (var f in files)
            {
                var source = File.ReadAllBytes(f);
                var compressedData = Snappy.Compress(source);
                var decompressedData = Snappy.Decompress(compressedData, 0, compressedData.Length);
                Assert.True(decompressedData.SequenceEqual(source));
            }
        }
    }
}