using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Cassandra.Collections;

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
        private const int ExpiredState = 1;
        private const int CancelledState = 2;
        private readonly Bucket[] _wheel;
        private readonly Timer _timer;
        private int _isDisposed;
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
            _timer = new Timer(TimerTick, null, _tickDuration, Timeout.Infinite);
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
        /// Releases the underlying timer instance.
        /// </summary>
        public void Dispose()
        {
            //Allow multiple calls to dispose
            if (Interlocked.Increment(ref _isDisposed) != 1)
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
            Tuple<TimeoutItem, long> pending;
            while (_pendingToAdd.TryDequeue(out pending))
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
            TimeoutItem timeout;
            while (_cancelledTimeouts.TryDequeue(out timeout))
            {
                timeout.Bucket.Remove(timeout);
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
                //there should not be any reference to the item in the bucket
                //Break references to make GC easier
                item.Dispose();
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
        internal interface ITimeout : IDisposable
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

            public void Dispose()
            {
                DoDispose();
                GC.SuppressFinalize(this);
            }

            private void DoDispose()
            {
                //Break references
                Next = null;
                Previous = null;
                Bucket = null;
                _actionState = null;
                _action = null;
                _timer = null;
            }

            ~TimeoutItem()
            {
                DoDispose();
            }
        }
    }
}
