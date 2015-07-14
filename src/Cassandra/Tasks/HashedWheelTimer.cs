using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
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
    internal class HashedWheelTimer : IDisposable
    {
        private const int InitState = 0;
        private const int ExpiredState = 1;
        private const int CancelledState = 2;
        private readonly Bucket[] _wheel;
        private readonly Timer _timer;
        private int _isDisposed;
        private readonly int _tickDuration;
        private readonly ConcurrentQueue<Tuple<TimeoutItem, long>> _pendingToAdd = new ConcurrentQueue<Tuple<TimeoutItem, long>>();

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
            Index = (Index + 1)%_wheel.Length;
            _timer.Change(_tickDuration, Timeout.Infinite);
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
        public ITimeout NewTimeout(Action action, long delay)
        {
            if (delay < _tickDuration)
            {
                delay = _tickDuration;
            }
            var item = new TimeoutItem(action);
            _pendingToAdd.Enqueue(Tuple.Create(item, delay));
            return item;
        }

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
        /// Linked list of Timeouts to allow easy removal of HashedWheelTimeouts in the middle.
        /// Methods are not thread safe.
        /// </summary>
        internal class Bucket: IEnumerable<TimeoutItem>
        {
            internal TimeoutItem Head { get; private set; }

            internal TimeoutItem Tail { get; private set; }

            internal void Add(TimeoutItem item)
            {
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
                    Tail = Head.Next;
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

        internal class TimeoutItem : ITimeout
        {
            private int _state = InitState;

            internal Action Action { get; private set; }

            internal long Rounds { get; set; }

            internal TimeoutItem Next { get; set; }

            internal TimeoutItem Previous { get; set; }

            public bool IsCancelled
            {
                get { return _state == CancelledState; }
            }

            internal TimeoutItem(Action action)
            {
                Action = action;
            }

            public bool Cancel()
            {
                return Interlocked.CompareExchange(ref _state, CancelledState, InitState) == InitState;
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
                Action();
            }
        }
    }
}
