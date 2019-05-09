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

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Tasks;

namespace Cassandra.ProtocolEvents
{
    /// <summary>
    /// Timer that implements a sliding window style algorithm of delay which enables the timer to keep
    /// postponing the action trigger while the <see cref="SlideDelay"/> keeps getting invoked (up to a maximum specified delay).
    /// It also schedules all internal code execution with an exclusive scheduler and exposes it through <see cref="ExclusiveScheduler"/>.
    /// </summary>
    internal class SlidingWindowExclusiveTimer : IDisposable
    {
        private readonly ITimerFactory _timerFactory;
        private readonly TimeSpan _delayIncrement;
        private readonly TimeSpan _maxDelay;
        private readonly Action _act;
        private volatile ITimer _timer;
        private long _counter;
        private long _windowFirstTimestamp;
        private volatile bool _isRunning;
        private readonly TaskFactory _timerTaskFactory;

        public SlidingWindowExclusiveTimer(ITimerFactory timerFactory, TimeSpan delayIncrement, TimeSpan maxDelay, Action act)
        {
            if (delayIncrement > maxDelay)
            {
                throw new ArgumentException("delayIncrement can not be greater than maxDelay");
            }
            
            var scheduler = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
            var taskFactory = new TaskFactory(
                CancellationToken.None,
                TaskCreationOptions.DenyChildAttach,
                TaskContinuationOptions.DenyChildAttach,
                scheduler);

            ExclusiveScheduler = scheduler;
            _timerTaskFactory = taskFactory;
            _timerFactory = timerFactory;
            _delayIncrement = delayIncrement;
            _maxDelay = maxDelay;
            _act = act;
            _isRunning = false;
        }

        public TaskScheduler ExclusiveScheduler { get; }

        public void Dispose()
        {
            _timer?.Dispose();
        }
        
        public void SlideDelay(bool processNow)
        {
            // delegate can't be async or shared exclusive scheduler is pointless
            _timerTaskFactory.StartNew(() =>
            {
                var currentTimestamp = Stopwatch.GetTimestamp();
                var timeUntilNextTrigger = processNow ? TimeSpan.Zero : ComputeTimeUntilNextTrigger(currentTimestamp);

                // if the computation resulted in Zero, then call _act() now and skip creating the timer
                if (timeUntilNextTrigger <= TimeSpan.Zero)
                {
                    CancelExistingTimer();
                    _act();

                    // increment counter so that previous timers don't execute _act()
                    IncrementTimerCounter();

                    _isRunning = false;
                    return;
                }

                CancelExistingTimer();

                // increment counter and pass the new counter to the new timer
                var counter = IncrementTimerCounter();
                _timer = _timerFactory.Create(Fire, counter, timeUntilNextTrigger, Timeout.InfiniteTimeSpan);

            }).Forget();
        }

        private TimeSpan ComputeTimeUntilNextTrigger(long currentTimestamp)
        {
            TimeSpan timeUntilNextTrigger;
            long diffTimestamp;

            // get time past since last trigger
            if (!_isRunning)
            {
                Volatile.Write(ref _windowFirstTimestamp, currentTimestamp);
                diffTimestamp = 0;
                _isRunning = true;
            }
            else
            {
                diffTimestamp = currentTimestamp - Volatile.Read(ref _windowFirstTimestamp);
            }

            var timeSinceLastTrigger = new TimeSpan(diffTimestamp);

            // compute time until next trigger based on how long has passed and _maxDelay
            if (timeSinceLastTrigger >= _maxDelay)
            {
                // if we already went past _maxDelay, trigger now
                timeUntilNextTrigger = TimeSpan.Zero;
            }
            else if (timeSinceLastTrigger.Add(_delayIncrement) >= _maxDelay)
            {
                // if the provided delay would make us go past _maxDelay, just trigger at the point of _maxDelay
                timeUntilNextTrigger = _maxDelay.Subtract(timeSinceLastTrigger);
            }
            else
            {
                timeUntilNextTrigger = _delayIncrement;
            }

            return timeUntilNextTrigger;
        }

        private void CancelExistingTimer()
        {
            // if there is an existing timer, cancel it (if by any change it already fired and the task is scheduled then 
            // the counter increment will make it not invoke _act()
            if (_timer != null)
            {
                _timer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
                _timer.Dispose();
                _timer = null;
            }
        }

        /// <summary>
        /// Increments timer counter so that we can avoid duplicate <see cref="_act"/> calls (<see cref="Fire"/> checks the provided counter)
        /// </summary>
        private long IncrementTimerCounter()
        {
            var currentCounter = Volatile.Read(ref _counter);
            currentCounter = (currentCounter == long.MaxValue) ? 0 : (currentCounter + 1);
            Volatile.Write(ref _counter, currentCounter);
            return currentCounter;
        }

        private void Fire(object state)
        {
            // delegate can't be async otherwise exclusive scheduler is pointless
            _timerTaskFactory.StartNew(() =>
            {
                if (_isRunning)
                {
                    var counter = Volatile.Read(ref _counter);
                    
                    // if counter is different then another timer was created or act() was already invoked in SlideDelay()
                    if (counter == (long)state)
                    {
                        _act();
                        _isRunning = false;
                    }
                }
            }).Forget();
        }
    }
}