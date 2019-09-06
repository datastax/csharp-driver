//
//       Copyright (C) DataStax Inc.
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
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

using Cassandra.ProtocolEvents.Internal;
using Cassandra.Tasks;

namespace Cassandra.ProtocolEvents
{
    /// <inheritdoc />
    internal class ProtocolEventDebouncer : IProtocolEventDebouncer
    {
        private static readonly Logger Logger = new Logger(typeof(ProtocolEventDebouncer));

        private readonly ActionBlock<Tuple<TaskCompletionSource<bool>, ProtocolEvent, bool>> _enqueueBlock;
        private readonly ActionBlock<EventQueue> _processQueueBlock;
        private readonly SlidingWindowExclusiveTimer _timer;

        private volatile EventQueue _queue = new EventQueue();

        public ProtocolEventDebouncer(ITimerFactory timerFactory, TimeSpan delay, TimeSpan maxDelay)
        {
            _timer = new SlidingWindowExclusiveTimer(timerFactory, delay, maxDelay, Process);

            // delegate for this block can't be async otherwise the shared exclusive scheduler is pointless
            _enqueueBlock = new ActionBlock<Tuple<TaskCompletionSource<bool>, ProtocolEvent, bool>>(tuple =>
            {
                try
                {
                    if (tuple.Item2 is KeyspaceProtocolEvent keyspaceEvent)
                    {
                        KeyspaceEventReceived(keyspaceEvent, tuple.Item3, tuple.Item1);
                    }
                    else
                    {
                        MainEventReceived(tuple.Item2, tuple.Item3, tuple.Item1);
                    }
                }
                catch (Exception ex)
                {
                    ProtocolEventDebouncer.Logger.Error("Unexpected exception in Protocol Event Debouncer while receiving events.", ex);
                }
            }, new ExecutionDataflowBlockOptions
            {
                EnsureOrdered = true,
                MaxDegreeOfParallelism = 1,
                TaskScheduler = _timer.ExclusiveScheduler
            });

            _processQueueBlock = new ActionBlock<EventQueue>(async queue =>
            {
                try
                {
                    await ProtocolEventDebouncer.ProcessQueue(queue).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    ProtocolEventDebouncer.Logger.Error("Unexpected exception in Protocol Event Debouncer while processing queue.", ex);
                }
            }, new ExecutionDataflowBlockOptions
            {
                EnsureOrdered = true,
                MaxDegreeOfParallelism = 1
            });
        }

        /// <inheritdoc />
        public Task ScheduleEventAsync(ProtocolEvent ev, bool processNow)
        {
            return _enqueueBlock.SendAsync(new Tuple<TaskCompletionSource<bool>, ProtocolEvent, bool>(null, ev, processNow));
        }

        /// <inheritdoc />
        public async Task HandleEventAsync(ProtocolEvent ev, bool processNow)
        {
            var callback = new TaskCompletionSource<bool>();
            await _enqueueBlock.SendAsync(new Tuple<TaskCompletionSource<bool>, ProtocolEvent, bool>(callback, ev, processNow)).ConfigureAwait(false);

            // continuewith very important because otherwise continuations run synchronously
            // https://stackoverflow.com/q/34658258/10896275
            var task = callback.Task.ContinueWith(x => x.Result, TaskScheduler.Default);
            await task.ConfigureAwait(false);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            _enqueueBlock.Complete();
            _enqueueBlock.Completion.GetAwaiter().GetResult();
            _timer.Dispose();
            _processQueueBlock.Complete();
            _processQueueBlock.Completion.GetAwaiter().GetResult();
        }

        // for tests
        internal EventQueue GetQueue() => _queue;

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

            if (ev.IsRefreshKeyspaceEvent)
            {
                keyspaceEvents.RefreshKeyspaceEvent = ev;
            }

            keyspaceEvents.Events.Add(new InternalKeyspaceProtocolEvent { Callback = callback, KeyspaceEvent = ev });
            _timer.SlideDelay(processNow);
        }

        private void Process()
        {
            if (_queue == null)
            {
                return;
            }

            // this is running with the exclusive scheduler so this is fine
            var queue = _queue;
            _queue = null;

            // not necessary to enqueue within the exclusive scheduler
            Task.Run(async () =>
            {
                try
                {
                    await _processQueueBlock.SendAsync(queue).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    ProtocolEventDebouncer.Logger.Error("EventDebouncer timer callback threw an exception.", ex);
                }
            }).Forget();
        }

        private static async Task ProcessQueue(EventQueue queue)
        {
            if (queue.MainEvent != null)
            {
                try
                {
                    await queue.MainEvent.Handler().ConfigureAwait(false);
                    foreach (var cb in queue.Callbacks)
                    {
                        cb?.TrySetResult(true);
                    }
                }
                catch (Exception ex)
                {
                    foreach (var cb in queue.Callbacks)
                    {
                        cb?.TrySetException(ex);
                    }
                }
                return;
            }

            foreach (var keyspace in queue.Keyspaces)
            {
                if (keyspace.Value.RefreshKeyspaceEvent != null)
                {
                    try
                    {
                        await keyspace.Value.RefreshKeyspaceEvent.Handler().ConfigureAwait(false);
                        foreach (var cb in keyspace.Value.Events.Select(e => e.Callback).Where(e => e != null))
                        {
                            cb.TrySetResult(true);
                        }
                    }
                    catch (Exception ex)
                    {
                        foreach (var cb in keyspace.Value.Events.Select(e => e.Callback).Where(e => e != null))
                        {
                            cb.TrySetException(ex);
                        }
                    }
                }

                foreach (var ev in keyspace.Value.Events)
                {
                    try
                    {
                        await ev.KeyspaceEvent.Handler().ConfigureAwait(false);
                        ev.Callback?.TrySetResult(true);
                    }
                    catch (Exception ex)
                    {
                        ev.Callback?.TrySetException(ex);
                    }
                }
            }
        }
    }
}