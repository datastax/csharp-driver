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
using System.Collections.Generic;

namespace Cassandra.Data.Linq
{
    internal class VisitingParam<T>
    {
        private readonly Stack<T> Stack = new Stack<T>();
        private readonly T def;

        public VisitingParam(T def)
        {
            this.def = def;
        }

        public IDisposable set(T val)
        {
            return new Lock(Stack, val);
        }

        public IDisposable setIf(bool cond, T val)
        {
            if (cond)
                return new Lock(Stack, val);
            return new NullLock();
        }

        public T get()
        {
            if (Stack.Count == 0) return def;
            return Stack.Peek();
        }

        private class Lock : IDisposable
        {
            private readonly Stack<T> Stack;

            public Lock(Stack<T> Stack, T val)
            {
                this.Stack = Stack;
                this.Stack.Push(val);
            }

            void IDisposable.Dispose()
            {
                Stack.Pop();
            }
        }

        private class NullLock : IDisposable
        {
            void IDisposable.Dispose()
            {
            }
        }
    }
}