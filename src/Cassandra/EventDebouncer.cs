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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Cassandra.Tasks;

namespace Cassandra
{
////    /**
//// * @param {Number} delay
//// * @private
//// * */
////EventDebouncer.prototype._slideDelay = function (delay) {
////  const self = this;
////  function process() {
////    const q = self._queue;
////    self._queue = null;
////    self._timeout = null;
////    processQueue(q);
////  }
////  if (delay === 0) {
////    // no delay, process immediately
////    if (this._timeout) {
////      clearTimeout(this._timeout);
////    }
////    return process();
////  }
////  const previousTimeout = this._timeout;
////  // add the new timeout before removing the previous one performs better
////  this._timeout = setTimeout(process, delay);
////  if (previousTimeout) {
////    clearTimeout(previousTimeout);
////  }
////};

/////**
//// * Adds a new event to the queue and moves the delay.
//// * @param {{ handler: Function, all: boolean|undefined, keyspace: String|undefined, cqlObject: String|null|undefined,
//// * callback: Function|undefined }} event
//// * @param {Boolean} processNow
//// */
////EventDebouncer.prototype.eventReceived = function (event, processNow) {
////  event.callback = event.callback || utils.noop;
////  this._queue = this._queue || { callbacks: [], keyspaces: {} };
////  const delay = !processNow ? this._delay : 0;
////  if (event.all) {
////    // when an event marked with all is received, it supersedes all the rest of events
////    // a full update (hosts + keyspaces + tokens) is going to be made
////    this._queue.mainEvent = event;
////  }
////  if (this._queue.callbacks.length === _queueOverflowThreshold) {
////    // warn once
////    this._logger('warn', util.format('Event debouncer queue exceeded %d events', _queueOverflowThreshold));
////  }
////  this._queue.callbacks.push(event.callback);
////  if (this._queue.mainEvent) {
////    // a full refresh is scheduled and the callback was added, nothing else to do.
////    return this._slideDelay(delay);
////  }
////  // Insert at keyspace level
////  let keyspaceEvents = this._queue.keyspaces[event.keyspace];
////  if (!keyspaceEvents) {
////    keyspaceEvents = this._queue.keyspaces[event.keyspace] = { events: [] };
////  }
////  if (event.cqlObject === undefined) {
////    // a full refresh of the keyspace, supersedes all child keyspace events
////    keyspaceEvents.mainEvent = event;
////  }
////  keyspaceEvents.events.push(event);
////  this._slideDelay(delay);
////};

    internal class ProtocolEventDebouncer : IProtocolEventDebouncer
    {
        private readonly ActionBlock<Tuple<TaskCompletionSource<bool>, ProtocolEvent, bool>> _enqueueBlock;
        private readonly ActionBlock<EventQueue> _processQueueBlock;
        private readonly CustomTimer _timer;

        private volatile EventQueue _queue = new EventQueue();

        public ProtocolEventDebouncer(ITimerFactory timerFactory, TimeSpan delay, TimeSpan maxDelay)
        {
            var scheduler = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
            var taskFactory = new TaskFactory(
                CancellationToken.None, 
                TaskCreationOptions.DenyChildAttach,
                TaskContinuationOptions.DenyChildAttach, 
                scheduler);

            _timer = new CustomTimer(taskFactory, timerFactory, delay, maxDelay, Process);

            // delegate for this block can't be async otherwise the shared exclusive scheduler is pointless
            _enqueueBlock = new ActionBlock<Tuple<TaskCompletionSource<bool>, ProtocolEvent, bool>>(tuple =>
            {
                if (tuple.Item2 is KeyspaceProtocolEvent keyspaceEvent)
                {
                    KeyspaceEventReceived(keyspaceEvent, tuple.Item3, tuple.Item1);
                }
                else
                {
                    MainEventReceived(tuple.Item2, tuple.Item3, tuple.Item1);
                }
            }, new ExecutionDataflowBlockOptions
            {
                EnsureOrdered = true, 
                MaxDegreeOfParallelism = 1,
                TaskScheduler = scheduler
            });

            _processQueueBlock = new ActionBlock<EventQueue>(async queue =>
            {
                await ProtocolEventDebouncer.ProcessQueue(queue).ConfigureAwait(false);
            }, new ExecutionDataflowBlockOptions
            {
                EnsureOrdered = true,
                MaxDegreeOfParallelism = 1
            });
        }

        public Task ScheduleEventAsync(ProtocolEvent ev, bool processNow)
        {
            return _enqueueBlock.SendAsync(new Tuple<TaskCompletionSource<bool>, ProtocolEvent, bool>(null, ev, processNow));
        }

        public async Task HandleEventAsync(ProtocolEvent ev, bool processNow)
        {
            var callback = new TaskCompletionSource<bool>();
            await _enqueueBlock.SendAsync(new Tuple<TaskCompletionSource<bool>, ProtocolEvent, bool>(callback, ev, processNow)).ConfigureAwait(false);

            // continuewith very important because otherwise continuations run synchronously
            // https://stackoverflow.com/q/34658258/10896275
            var task = callback.Task.ContinueWith(x => x.Result, TaskScheduler.Default);
            await task.ConfigureAwait(false);
        }

        private void MainEventReceived(ProtocolEvent ev, bool processNow, TaskCompletionSource<bool> callback)
        {
            if (_queue == null)
            {
                _queue = new EventQueue();
            }

            _queue.MainEvent = ev;
            if (callback != null)
            {
                _queue.Callbacks.Add(callback);
            }
            _timer.SlideDelay(processNow);
        }

        private void KeyspaceEventReceived(KeyspaceProtocolEvent ev, bool processNow, TaskCompletionSource<bool> callback)
        {
            if (_queue == null)
            {
                _queue = new EventQueue();
            }

            if (callback != null)
            {
                _queue.Callbacks.Add(callback);
            }

            if (_queue.MainEvent != null)
            {
                _timer.SlideDelay(processNow);
                return;
            }

            if (!_queue.Keyspaces.TryGetValue(ev.Keyspace, out var keyspaceEvents))
            {
                keyspaceEvents = new KeyspaceEvents();
                _queue.Keyspaces.Add(ev.Keyspace, keyspaceEvents);
            }

            if (ev.MainEvent)
            {
                keyspaceEvents.MainEvent = ev;
            }

            keyspaceEvents.Events.Add(new InternalKeyspaceProtocolEvent { Callback = callback, KeyspaceEvent = ev });
            _timer.SlideDelay(processNow);
        }

        ////    /**
//// * @param {Number} delay
//// * @private
//// * */
////EventDebouncer.prototype._slideDelay = function (delay) {
////  const self = this;
////  function process() {
////    const q = self._queue;
////    self._queue = null;
////    self._timeout = null;
////    processQueue(q);
////  }
////  if (delay === 0) {
////    // no delay, process immediately
////    if (this._timeout) {
////      clearTimeout(this._timeout);
////    }
////    return process();
////  }
////  const previousTimeout = this._timeout;
////  // add the new timeout before removing the previous one performs better
////  this._timeout = setTimeout(process, delay);
////  if (previousTimeout) {
////    clearTimeout(previousTimeout);
////  }
////};

/////**
//// * Adds a new event to the queue and moves the delay.
//// * @param {{ handler: Function, all: boolean|undefined, keyspace: String|undefined, cqlObject: String|null|undefined,
//// * callback: Function|undefined }} event
//// * @param {Boolean} processNow
//// */

        private void Process()
        {
            // this is running with the exclusive scheduler so this is fine
            var queue = _queue;
            _queue = null;

            // this block is not running with the exclusive scheduler so blocking here is fine
            _processQueueBlock.SendAsync(queue).GetAwaiter().GetResult();
        }
        
/////**
//// * @param {{callbacks: Array, keyspaces: Object, mainEvent: Object}} q
//// * @private
//// */
////function processQueue (q) {
////  if (q.mainEvent) {
////    // refresh all by invoking 1 handler and invoke all pending callbacks
////    return q.mainEvent.handler(function invokeCallbacks(err) {
////      for (let i = 0; i < q.callbacks.length; i++) {
////        q.callbacks[i](err);
////      }
////    });
////  }
////  utils.each(Object.keys(q.keyspaces), function eachKeyspace(name, next) {
////    const keyspaceEvents = q.keyspaces[name];
////    if (keyspaceEvents.mainEvent) {
////      // refresh a keyspace
////      return keyspaceEvents.mainEvent.handler(function mainEventCallback(err) {
////        for (let i = 0; i < keyspaceEvents.events.length; i++) {
////          keyspaceEvents.events[i].callback(err);
////        }
////        next();
////      });
////    }
////    // deal with individual handlers and callbacks
////    keyspaceEvents.events.forEach(function eachEvent(event) {
////      // sync handlers
////      event.handler();
////      event.callback();
////    });
////    next();
////  });
////}
        ///

        private static async Task ProcessQueue(EventQueue queue)
        {
            if (queue.MainEvent != null)
            {
                await queue.MainEvent.Handler().ConfigureAwait(false);
                foreach (var cb in queue.Callbacks)
                {
                    cb?.SetResult(true);
                }
                return;
            }

            foreach (var keyspace in queue.Keyspaces)
            {
                if (keyspace.Value.MainEvent != null)
                {
                    await keyspace.Value.MainEvent.Handler().ConfigureAwait(false);
                    foreach (var cb in keyspace.Value.Events.Select(e => e.Callback).Where(e => e != null))
                    {
                        cb.SetResult(true);
                    }
                    return;
                }

                foreach (var ev in keyspace.Value.Events)
                {
                    await ev.KeyspaceEvent.Handler().ConfigureAwait(false);
                    ev.Callback?.SetResult(true);
                }
            }
        }

        private class InternalKeyspaceProtocolEvent
        {
            public KeyspaceProtocolEvent KeyspaceEvent { get; set; }

            public TaskCompletionSource<bool> Callback { get; set; }
        }

        private class EventQueue
        {
            public volatile ProtocolEvent MainEvent;

            public IList<TaskCompletionSource<bool>> Callbacks { get; } = new List<TaskCompletionSource<bool>>();

            public IDictionary<string, KeyspaceEvents> Keyspaces { get; } = new Dictionary<string, KeyspaceEvents>();
        }

        private class KeyspaceEvents
        {
            public volatile ProtocolEvent MainEvent;

            public IList<InternalKeyspaceProtocolEvent> Events { get; } = new List<InternalKeyspaceProtocolEvent>();
        }
    }

    internal interface IProtocolEventDebouncer
    {
        /// <summary>
        /// Returned task will be complete when the event has been scheduled for processing.
        /// </summary>
        Task ScheduleEventAsync(ProtocolEvent ev, bool processNow);

        /// <summary>
        /// Returned task will be complete when the event has been processed.
        /// </summary>
        Task HandleEventAsync(ProtocolEvent ev, bool processNow);
    }

    internal class CustomTimer
    {
        private readonly ITimerFactory _timerFactory;
        private readonly TimeSpan _delayIncrement;
        private readonly TimeSpan _maxDelay;
        private readonly Action _act;
        private volatile ITimer _timer;
        private long _counter;
        private long _lastDelayTimestamp;
        private volatile bool _isRunning;
        private readonly TaskFactory _timerTaskFactory;

        public CustomTimer(TaskFactory taskFactory, ITimerFactory timerFactory, TimeSpan delayIncrement, TimeSpan maxDelay, Action act)
        {
            if (delayIncrement > maxDelay)
            {
                throw new ArgumentException("delayIncrement can not be greater than maxDelay");
            }

            _timerTaskFactory = taskFactory;
            _timerFactory = timerFactory;
            _delayIncrement = delayIncrement;
            _maxDelay = maxDelay;
            _act = act;
            _isRunning = false;
        }

        public void SlideDelay(bool processNow)
        {
            // delegate can't be async or shared exclusive scheduler is pointless
            _timerTaskFactory.StartNew(() =>
            {
                var timeSpan = _delayIncrement;

                if (processNow)
                {
                    timeSpan = TimeSpan.Zero;
                }
                else
                {
                    var currentTimestamp = Stopwatch.GetTimestamp();
                    long diffTimestamp;
                    if (!_isRunning)
                    {
                        Interlocked.Exchange(ref _lastDelayTimestamp, currentTimestamp);
                        diffTimestamp = 0;
                    }
                    else
                    {
                        var lastDelayTimestamp = Interlocked.Read(ref _lastDelayTimestamp);
                        diffTimestamp = currentTimestamp - lastDelayTimestamp;
                    }

                    var diffTimeSpan = new TimeSpan(diffTimestamp);
                    if (diffTimeSpan >= _maxDelay)
                    {
                        timeSpan = TimeSpan.Zero;
                    }
                    else if (diffTimeSpan.Add(timeSpan) >= _maxDelay)
                    {
                        timeSpan = _maxDelay.Subtract(diffTimeSpan);
                    }
                }

                if (timeSpan == TimeSpan.Zero)
                {
                    if (_isRunning)
                    {
                        _timer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
                        _timer.Dispose();
                        _timer = null;
                    }

                    _act();
                    _isRunning = false;
                    return;
                }

                if (_isRunning)
                {
                    _timer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
                    _timer.Dispose();
                }
                else
                {
                    _isRunning = true;
                }

                var counter = Interlocked.Read(ref _counter);
                counter = (counter == long.MaxValue) ? 0 : (counter + 1);
                Interlocked.Exchange(ref _counter, counter);

                _timer = _timerFactory.Create(Fire, counter, timeSpan, Timeout.InfiniteTimeSpan);
            }).Forget();
        }
        
        private void Fire(object state)
        {
            // delegate can't be async otherwise exclusive scheduler is pointless
            _timerTaskFactory.StartNew(() => 
            {
                if (_isRunning)
                {
                    var counter = Interlocked.Read(ref _counter);
                    if (counter == (long) state)
                    {
                        _act();
                        _isRunning = false;
                    }
                }
            }).Forget();
        }
    }

    internal interface ITimerFactory
    {
        ITimer Create(TimerCallback action, object state, TimeSpan due, TimeSpan period);
    }

    internal interface ITimer : IDisposable
    {
        bool Change(TimeSpan due, TimeSpan period);
    }

    internal class DotnetTimerFactory : ITimerFactory
    {
        public ITimer Create(TimerCallback action, object state, TimeSpan due, TimeSpan period)
        {
            return new DotnetTimer(action, state, due, period);
        }
    }

    internal class DotnetTimer : ITimer
    {
        private readonly Timer _timer;

        public DotnetTimer(TimerCallback action, object state, TimeSpan due, TimeSpan period)
        {
            _timer = new Timer(action, state, due, period);
        }

        public void Dispose()
        {
            _timer.Dispose();
        }

        public bool Change(TimeSpan due, TimeSpan period)
        {
            return _timer.Change(due, period);
        }
    }

    internal class ProtocolEvent
    {
        public ProtocolEvent(Func<Task> handler)
        {
            Handler = handler;
        }

        public Func<Task> Handler { get; }
    }

    internal class KeyspaceProtocolEvent : ProtocolEvent
    {
        public KeyspaceProtocolEvent(bool mainEvent, string keyspace, Func<Task> handler)
            : base(handler)
        {
            MainEvent = mainEvent;
            Keyspace = keyspace;
        }

        public bool MainEvent { get; }

        public string Keyspace { get; }
    }
}