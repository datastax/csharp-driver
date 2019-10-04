// 
//       Copyright (C) 2019 DataStax Inc.
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
// 

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace Cassandra.Collections
{
    /// <summary>
    /// Wrapper for the ConcurrentStack that implements fast Count query that works in constant time but provide slightly less guarantees:
    /// count can be out of sync with any stack state because of concurrency, but it is always not less than actual stack size and in general
    /// it can provide fairly accurate information about size
    /// </summary>
    internal class CountableConcurrentStack<T>
    {
        private readonly ConcurrentStack<T> _stack;
        private int _count;

        public int Count => Volatile.Read(ref _count);

        public bool IsEmpty => _stack.IsEmpty;

        public CountableConcurrentStack(IEnumerable<T> values)
        {
            _stack = new ConcurrentStack<T>(values);
            _count = _stack.Count;
        }

        public bool TryPop(out T value)
        {
            var popResult = _stack.TryPop(out value);
            if (popResult)
            {
                Interlocked.Decrement(ref _count);
            }

            return popResult;
        }

        public void Push(T value)
        {
            Interlocked.Increment(ref _count);
            _stack.Push(value);
        }
    }
}