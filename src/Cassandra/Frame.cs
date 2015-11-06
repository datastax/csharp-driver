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

namespace Cassandra
{
    internal class Frame
    {
        /// <summary>
        /// The 8 byte protocol header
        /// </summary>
        public FrameHeader Header { get; set; }

        /// <summary>
        /// A stream containing the frame body
        /// </summary>
        public Stream Body { get; set; }

        public Frame(FrameHeader header, Stream body)
        {
            if (header == null) throw new ArgumentNullException("header");
            if (body == null) throw new ArgumentNullException("body");

            Header = header;
            Body = body;
        }
    }
}