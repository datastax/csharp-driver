//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections;
using System.Collections.Generic;

namespace Dse.Data.Linq
{
    /// <summary>
    /// Represents nested states
    /// </summary>
    internal class VisitingParam<T>
    {
        private readonly Stack<T> _clauses = new Stack<T>();
        private readonly T _defaultValue;

        public VisitingParam(T defaultValue)
        {
            _defaultValue = defaultValue;
        }

        public VisitingParam() : this(default(T))
        {
            
        }

        public IDisposable Set(T val)
        {
            return new ClauseLock(_clauses, val);
        }

        public T Get()
        {
            return _clauses.Count == 0 ? _defaultValue : _clauses.Peek();
        }

        private class ClauseLock : IDisposable
        {
            private readonly Stack<T> _stack;

            public ClauseLock(Stack<T> stack, T val)
            {
                this._stack = stack;
                this._stack.Push(val);
            }

            void IDisposable.Dispose()
            {
                _stack.Pop();
            }
        }
    }
}
