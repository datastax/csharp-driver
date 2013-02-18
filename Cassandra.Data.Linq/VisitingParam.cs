using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.Data.Linq
{
    internal class VisitingParam<T>
    {
        Stack<T> Stack = new Stack<T>();
        T def;

        public VisitingParam(T def) { this.def = def; }

        class Lock : IDisposable
        {
            Stack<T> Stack;
            public Lock(Stack<T> Stack, T val)
            {
                this.Stack = Stack;
                this.Stack.Push(val);
            }
            void IDisposable.Dispose()
            {
                this.Stack.Pop();
            }
        }

        class NullLock : IDisposable
        {
            void IDisposable.Dispose()
            {
            }
        }

        public IDisposable set(T val)
        {
            return new Lock(Stack, val);
        }

        public IDisposable setIf(bool cond, T val)
        {
            if (cond)
                return new Lock(Stack, val);
            else
                return new NullLock();
        }

        public T get()
        {
            if (Stack.Count == 0) return def;
            else return Stack.Peek();
        }
    }
}
