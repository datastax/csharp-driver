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

        private volatile EventQueue _queue = null;

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
        public async Task ScheduleEventAsync(ProtocolEvent ev, bool processNow)
        {
            var sent = await _enqueueBlock
                             .SendAsync(new Tuple<TaskCompletionSource<bool>, ProtocolEvent, bool>(null, ev, processNow))
                             .ConfigureAwait(false);

            if (!sent)
            {
                throw new DriverInternalError("Could not schedule event in the ProtocolEventDebouncer.");
            }
        }

        /// <inheritdoc />
        public async Task HandleEventAsync(ProtocolEvent ev, bool processNow)
        {
            var callback = new TaskCompletionSource<bool>();
            var sent = await _enqueueBlock.SendAsync(new Tuple<TaskCompletionSource<bool>, ProtocolEvent, bool>(callback, ev, processNow)).ConfigureAwait(false);
            
            if (!sent)
            {
                throw new DriverInternalError("Could not schedule event in the ProtocolEventDebouncer.");
            }

            await callback.Task.ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task ShutdownAsync()
        {
            _enqueueBlock.Complete();
            await _enqueueBlock.Completion.ConfigureAwait(false);

            await _timer.SlideDelayAsync(true).ConfigureAwait(false);
            _timer.Dispose();

            _processQueueBlock.Complete();
            await _processQueueBlock.Completion.ConfigureAwait(false);
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
                var sent = false;
                try
                {
                    sent = await _processQueueBlock.SendAsync(queue).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    ProtocolEventDebouncer.Logger.Error("EventDebouncer timer callback threw an exception.", ex);
                }

                if (!sent)
                {
                    foreach (var cb in queue.Callbacks)
                    {
                        cb?.TrySetException(new DriverInternalError("Could not process events in the ProtocolEventDebouncer."));
                    }
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
                        if (cb != null)
                        {
                            Task.Run(() => cb.TrySetResult(true)).Forget();
                        }
                    }
                }
                catch (Exception ex)
                {
                    foreach (var cb in queue.Callbacks)
                    {
                        if (cb != null)
                        {
                            Task.Run(() => cb.TrySetException(ex)).Forget();
                        }
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
                            Task.Run(() => cb.TrySetResult(true)).Forget();
                        }
                    }
                    catch (Exception ex)
                    {
                        foreach (var cb in keyspace.Value.Events.Select(e => e.Callback).Where(e => e != null))
                        {
                            Task.Run(() => cb.TrySetException(ex)).Forget();
                        }
                    }

                    continue;
                }

                foreach (var ev in keyspace.Value.Events)
                {
                    try
                    {
                        await ev.KeyspaceEvent.Handler().ConfigureAwait(false);
                        if (ev.Callback != null)
                        {
                            Task.Run(() => ev.Callback.TrySetResult(true)).Forget();
                        }
                    }
                    catch (Exception ex)
                    {
                        if (ev.Callback != null)
                        {
                            Task.Run(() => ev.Callback.TrySetException(ex)).Forget();
                        }
                    }
                }
            }
        }
    }
}