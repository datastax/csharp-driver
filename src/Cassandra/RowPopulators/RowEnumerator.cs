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

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra
{
    /// <summary>
    /// Represents an enumerator that fires an event when  moved to end
    /// </summary>
    public class RowEnumerator : IEnumerator<Row>
    {
        private readonly List<Row> _list;
        private int _idx = -1;

        /// <summary>
        /// Event that is fired when the cursor pased all local rows in list
        /// </summary>
        public event Action MovedToEnd;

        public RowEnumerator(List<Row> list)
        {
            _list = list;
        }

        public Row Current
        {
            get
            {
                if (_idx == -1 || _idx >= _list.Count) return null;
                return _list[_idx];
            }
        }

        object IEnumerator.Current
        {
            get
            {
                return this.Current;
            }
        }

        public bool MoveNext()
        {
            _idx++;
            var canMove = _idx < _list.Count;
            if (!canMove)
            {
                if (MovedToEnd != null)
                {
                    //fire the event that stating that the cursor pased all local rows in list
                    MovedToEnd();
                }
                //Check again, after fetching, if its possible to move
                canMove = _idx < _list.Count;
            }
            return canMove;
        }

        public void Reset()
        {
            _idx = -1;
        }

        public void Dispose()
        {
        }
    }
}
