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
using System.Collections.Generic;

namespace Cassandra.Tests.Mapping.Pocos
{
    public class UdtAndTuplePoco
    {
        public Guid Id1 { get; set; }

        public Tuple<long, long, string> Tuple1 { get; set; }

        public Song Udt1 { get; set; }

        public SortedSet<Song> UdtSet1 { get; set; }

        public List<Song> UdtList1 { get; set; }

        public SortedDictionary<Tuple<double, double>, string> TupleMapKey1 { get; set; }

        public SortedDictionary<string, Tuple<double, double>> TupleMapValue1 { get; set; }

        public SortedDictionary<string, List<int>> ListMapValue1 { get; set; }
    }
}
