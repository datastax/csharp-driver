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
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace Cassandra.Tasks
{
    /// <summary>
    /// A reusable low precision timer with approximate scheduling
    /// </summary>
    /// <remarks>
    /// <para>
    /// Timeout actions are executed on a ThreadPool thread supplied by the system. If you need to execute blocking operations, 
    /// it is recommended that you start a new Task using a TaskScheduler.
    /// </para>
    /// Based on George Varghese and Tony Lauck's paper, <a href="http://cseweb.ucsd.edu/users/varghese/PAPERS/twheel.ps.Z"> 
    /// Hashed and Hierarchical Timing Wheels: data structures to efficiently implement a timer facility</a>
    /// </remarks>
    internal sealed class HashedWheelTimer : IDisposable
    {
        private const int InitState = 0;
        private const int StartedState = 1;
        private const int DisposedState = 2;
        private int _state;
        private readonly Bucket[] _wheel;
        private readonly Timer _timer;
        private readonly int _tickDuration;
        private readonly ConcurrentQueue<Tuple<TimeoutItem, long>> _pendingToAdd = new ConcurrentQueue<Tuple<TimeoutItem, long>>();
        private readonly ConcurrentQueue<TimeoutItem> _cancelledTimeouts = new ConcurrentQueue<TimeoutItem>();

        /// <summary>
        /// Represents the index of the next tick
        /// </summary>
        internal int Index { get; set; }

        public HashedWheelTimer(int tickDuration = 100, int ticksPerWheel = 512)
        {
            if (ticksPerWheel < 1)
            {
                throw new ArgumentOutOfRangeException("ticksPerWheel");
            }
            if (tickDuration < 20)
            {
                throw new ArgumentOutOfRangeException("tickDuration", "Timer resolution is system dependant, you should not use this class for tick durations lower than 20 ms");
            }
            //Create the wheel
            _wheel = new Bucket[ticksPerWheel];
            for (var i = 0; i < ticksPerWheel; i++)
            {
                _wheel[i] = new Bucket();
            }
            //Create the timer
            _tickDuration = tickDuration;
            _timer = new Timer(TimerTick, null, Timeout.Infinite, Timeout.Infinite);
        }

        private void TimerTick(object state)
        {
            AddPending();
            RemoveCancelled();
            //go through the timeouts in the current bucket and subtract the round
            //or expire
            var bucket = _wheel[Index];
            var timeout = bucket.Head;
            while (timeout != null)
            {
                if (timeout.Rounds == 0)
                {
                    timeout.Expire();
                    bucket.Remove(timeout);
                }
                else if (timeout.IsCancelled)
                {
                    bucket.Remove(timeout);
                }
                else
                {
                    timeout.Rounds--;
                }
                timeout = timeout.Next;
            }
            //Setup the next tick
            Index = (Index + 1) % _wheel.Length;
            SetTimer();
        }

        private void SetTimer()
        {
            try
            {
                _timer.Change(_tickDuration, Timeout.Infinite);
            }
            catch (ObjectDisposedException)
            {
                //the _timer might already have been disposed of
            }
        }
        
        /// <summary>
        /// Starts the timer explicitly.
        /// <para>Calls to <see cref="NewTimeout"/> will internally call this method.</para>
        /// </summary>
        public void Start()
        {
            // Only consider the case when it's not started and it should be.
            // We have to avoid atomic operations in the most common execution path, as they are somehow expensive
            // Use a volatile read first
            var state = Volatile.Read(ref _state);
            if (state == StartedState)
            {
                // Already started, ignore
                return;
            }
            var previousState = Interlocked.CompareExchange(ref _state, StartedState, InitState);
            if (previousState == DisposedState)
            {
                throw new InvalidOperationException("Can not start timer after Disposed");
            }
            if (previousState != InitState)
            {
                // Already started, ignore
                return;
            }
            SetTimer();
        }

        /// <summary>
        /// Releases the underlying timer instance.
        /// </summary>
        public void Dispose()
        {
            var previuosState = Interlocked.Exchange(ref _state, DisposedState);
            if (previuosState == DisposedState)
            {
                return;
            }
            _timer.Dispose();
        }

        /// <summary>
        /// Adds a new action to be executed with a delay
        /// </summary>
        /// <param name="action">
        /// Action to be executed. Consider that the action is going to be invoked in an IO thread.
        /// </param>
        /// <param name="state">Action state or null</param>
        /// <param name="delay">Delay in milliseconds</param>
        public ITimeout NewTimeout(Action<object> action, object state, long delay)
        {
            Start();
            if (delay < _tickDuration)
            {
                delay = _tickDuration;
            }
            var item = new TimeoutItem(this, action, state);
            _pendingToAdd.Enqueue(Tuple.Create(item, delay));
            return item;
        }

        /// <summary>
        /// Adds the timeouts to each bucket
        /// </summary>
        private void AddPending()
        {
            while (_pendingToAdd.TryDequeue(out Tuple<TimeoutItem, long> pending))
            {
                AddTimeout(pending.Item1, pending.Item2);
            }
        }

        private void AddTimeout(TimeoutItem item, long delay)
        {
            if (item.IsCancelled)
            {
                //It has been cancelled since then
                return;
            }
            //delay expressed in tickets
            var ticksDelay = delay / _tickDuration +
                //As index is for the current tick and it was added since the last tick
                Index - 1;
            var bucketIndex = Convert.ToInt32(ticksDelay % _wheel.Length);
            var rounds = ticksDelay / _wheel.Length;
            if (rounds > 0 && bucketIndex < Index)
            {
                rounds--;
            }
            item.Rounds = rounds;
            _wheel[bucketIndex].Add(item);
        }

        /// <summary>
        /// Removes all cancelled timeouts from the buckets
        /// </summary>
        private void RemoveCancelled()
        {
            while (_cancelledTimeouts.TryDequeue(out TimeoutItem timeout))
            {
                try
                {
                    timeout.Bucket.Remove(timeout);
                }
                catch (NullReferenceException)
                {
                    // The Bucket was already set to null: it was already removed, don't mind
                }
            }
        }

        /// <summary>
        /// Linked list of Timeouts to allow easy removal of HashedWheelTimeouts in the middle.
        /// Methods are not thread safe.
        /// </summary>
        internal sealed class Bucket : IEnumerable<TimeoutItem>
        {
            internal TimeoutItem Head { get; private set; }

            internal TimeoutItem Tail { get; private set; }

            internal void Add(TimeoutItem item)
            {
                item.Bucket = this;
                if (Tail == null)
                {
                    //is the first here
                    Head = Tail = item;
                    return;
                }
                Tail.Next = item;
                item.Previous = Tail;
                item.Next = null;
                Tail = item;
            }

            internal void Remove(TimeoutItem item)
            {
                if (item.Previous == null)
                {
                    Head = item.Next;
                    if (Head == null)
                    {
                        //it was the only element and the bucket is now empty
                        Tail = null;
                        return;
                    }
                    Head.Previous = null;
                    return;
                }
                item.Previous.Next = item.Next;
                if (item.Next == null)
                {
                    //it should be the tail
                    if (Tail != item)
                    {
                        throw new ArgumentException("Next is null but it is not the tail");
                    }
                    Tail = item.Previous;
                    Tail.Next = null;
                    return;
                }
                item.Next.Previous = item.Previous;
            }

            public IEnumerator<TimeoutItem> GetEnumerator()
            {
                if (Head == null)
                {
                    yield break;
                }
                var next = Head;
                yield return next;
                while ((next = next.Next) != null)
                {
                    yield return next;
                }
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }

        /// <summary>
        /// Represents an scheduled timeout
        /// </summary>
        internal interface ITimeout
        {
            bool IsCancelled { get; }

            /// <summary>
            /// Marks the timeout as cancelled if it hasn't expired yet.
            /// </summary>
            /// <returns>True if it has been cancelled by this call</returns>
            bool Cancel();
        }

        internal sealed class TimeoutItem : ITimeout
        {
            private const int ExpiredState = 1;
            private const int CancelledState = 2;
            //Use fields instead of properties as micro optimization
            //More 100 thousand timeout items could be created and GC collected each second
            private object _actionState;
            private Action<object> _action;
            private int _state = InitState;
            private HashedWheelTimer _timer;

            internal long Rounds;

            internal TimeoutItem Next;

            internal TimeoutItem Previous;

            internal Bucket Bucket;

            public bool IsCancelled
            {
                get { return _state == CancelledState; }
            }

            internal TimeoutItem(HashedWheelTimer timer, Action<object> action, object actionState)
            {
                _actionState = actionState;
                _action = action;
                _timer = timer;
            }

            public bool Cancel()
            {
                if (Interlocked.CompareExchange(ref _state, CancelledState, InitState) == InitState)
                {
                    if (Bucket != null)
                    {
                        //Mark this to be removed from the bucket on the next tick
                        _timer._cancelledTimeouts.Enqueue(this);
                    }
                    return true;
                }
                return false;
            }

            /// <summary>
            /// Execute the timeout action
            /// </summary>
            public void Expire()
            {
                if (Interlocked.CompareExchange(ref _state, ExpiredState, InitState) != InitState)
                {
                    //Its already cancelled
                    return;
                }
                _action(_actionState);
            }
        }
    }
}
