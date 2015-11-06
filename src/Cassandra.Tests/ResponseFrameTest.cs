//
//      Copyright (C) 2012-2014 DataStax Inc.
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
using System.IO;
using NUnit.Framework;

namespace Cassandra.Tests
{
    public class ResponseFrameTest
    {
        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void Ctor_HeaderIsNull_Throws()
        {
            new Frame(null, new MemoryStream());
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void Ctor_BodyIsNull_Throws()
        {
            new Frame(new FrameHeader(), null);
        } 
    }
}