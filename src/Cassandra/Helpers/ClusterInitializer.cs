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

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Connections.Control;
using Cassandra.Requests;
using Cassandra.SessionManagement;
using Cassandra.Tasks;

namespace Cassandra.Helpers
{
    internal class ClusterInitializer : IClusterInitializer
    {
        private const int Disposed = 10;
        private const int Initialized = 5;
        private const int Initializing = 1;

        private static readonly Logger Logger = new Logger(typeof(ClusterInitializer));

        private readonly IInternalCluster _cluster;
        private readonly IInternalMetadata _internalMetadata;
        private readonly CancellationTokenSource _initCancellationTokenSource = new CancellationTokenSource();
        private readonly IList<Func<Task>> _initCallbacks = new List<Func<Task>>();
        private readonly object _initCallbackLock = new object();

        private volatile bool _initialized = false;
        private volatile Task _initTask;
        private volatile TaskCompletionSource<IInternalMetadata> _initTaskCompletionSource 
            = new TaskCompletionSource<IInternalMetadata>();

        private volatile InitFatalErrorException _initException;

        private long _state = ClusterInitializer.Initializing;

        public ClusterInitializer(
            IInternalCluster cluster, IInternalMetadata internalMetadata)
        {
            _cluster = cluster;
            _internalMetadata = internalMetadata;
        }

        public void Initialize()
        {
            _initTask = Task.Run(InitInternalAsync);
        }

        /// <summary>
        /// Called by the control connection after initializing
        /// </summary>
        /// <returns></returns>
        public async Task PostInitializeAsync()
        {
            await _cluster.PostInitializeAsync().ConfigureAwait(false);
        }
        
        private static string GetAssemblyInfo()
        {
            var assembly = typeof(ISession).GetTypeInfo().Assembly;
            var info = FileVersionInfo.GetVersionInfo(assembly.Location);
            return $"{info.ProductName} v{info.FileVersion}";
        }

        public bool TryAttachInitCallback(Func<Task> callback)
        {
            if (_initialized)
            {
                return false;
            }
            
            if (IsDisposed)
            {
                return false;
            }

            lock (_initCallbackLock)
            {
                if (_initialized)
                {
                    return false;
                }

                if (IsDisposed)
                {
                    return false;
                }

                _initCallbacks.Add(callback);
                return true;
            }
        }

        public bool IsDisposed => Interlocked.Read(ref _state) == ClusterInitializer.Disposed;

        private async Task InitInternalAsync()
        {
            ClusterInitializer.Logger.Info("Connecting to cluster using {0}", GetAssemblyInfo());
            try
            {
                var reconnectionSchedule = _cluster.Configuration.Policies.ReconnectionPolicy.NewSchedule();

                do
                {
                    try
                    {
                        await _internalMetadata.ControlConnection.InitAsync(this).ConfigureAwait(false);
                        break;
                    }
                    catch (Exception ex)
                    {
                        if (IsDisposed)
                        {
                            throw new ObjectDisposedException("Cluster instance was disposed before initialization finished.");
                        }

                        var currentTcs = _initTaskCompletionSource;
                        
                        var delay = reconnectionSchedule.NextDelayMs();
                        ClusterInitializer.Logger.Error(ex, "Cluster initialization failed. Retrying in {0} ms.", delay);
                        Task.Run(() => currentTcs.TrySetException(ex)).Forget();

                        try
                        {
                            await Task.Delay(
                                TimeSpan.FromMilliseconds(delay), 
                                _initCancellationTokenSource.Token).ConfigureAwait(false);
                        }
                        finally
                        {
                            _initTaskCompletionSource = new TaskCompletionSource<IInternalMetadata>();
                        }
                        
                    }
                } while (!IsDisposed);
                
                if (IsDisposed)
                {
                    throw new ObjectDisposedException("Cluster instance was disposed before initialization finished.");
                }
                
                var previousState = Interlocked.CompareExchange(ref _state, ClusterInitializer.Initialized, ClusterInitializer.Initializing);
                if (previousState == ClusterInitializer.Disposed)
                {
                    await ShutdownInternalAsync().ConfigureAwait(false);
                    throw new ObjectDisposedException("Cluster instance was disposed before initialization finished.");
                }

                _initialized = true;
                
                ClusterInitializer.Logger.Info("Cluster Connected using binary protocol version: ["
                                    + _cluster.Configuration.SerializerManager.CurrentProtocolVersion
                                    + "]");
                ClusterInitializer.Logger.Info("Cluster [" + _internalMetadata.ClusterName + "] has been initialized.");

                Task.Run(() => _initTaskCompletionSource.TrySetResult(_internalMetadata)).Forget();

                Task.Run(async () =>
                {
                    IList<Func<Task>> callbacks;
                    lock (_initCallbackLock)
                    {
                        callbacks = new List<Func<Task>>(_initCallbacks);
                        _initCallbacks.Clear();
                    }

                    await Task.WhenAll(callbacks.Select(c => c.Invoke())).ConfigureAwait(false);
                }).Forget();
                return;
            }
            catch (Exception ex)
            {
                if (IsDisposed)
                {
                    var newEx = new ObjectDisposedException("Cluster instance was disposed before initialization finished.");
                    Task.Run(() => _initTaskCompletionSource.TrySetException(newEx)).Forget();
                    throw newEx;
                }

                //There was an error that the driver is not able to recover from
                //Store the exception for the following times
                _initException = new InitFatalErrorException(ex);
                //Throw the actual exception for the first time
                Task.Run(() => _initTaskCompletionSource.TrySetException(ex)).Forget();
                throw;
            }
        }
        
        public void WaitInit()
        {
            if (_initialized)
            {
                //It was already initialized
                return;
            }

            WaitInitInternal();
        }

        public IInternalMetadata WaitInitAndGetMetadata()
        {
            if (_initialized)
            {
                //It was already initialized
                return _internalMetadata;
            }

            return WaitInitInternal();
        }

        public Task<IInternalMetadata> WaitInitAndGetMetadataAsync()
        {
            if (_initialized)
            {
                //It was already initialized
                return Task.FromResult(_internalMetadata);
            }

            return WaitInitInternalAsync();
        }

        public Task WaitInitAsync()
        {
            if (_initialized)
            {
                //It was already initialized
                return TaskHelper.Completed;
            }

            return WaitInitInternalAsync();
        }
        private IInternalMetadata WaitInitInternal()
        {
            ValidateState();

            using (var waiter = new TaskTimeoutHelper<IInternalMetadata>(
                _initTaskCompletionSource.Task, _cluster.GetInitTimeout()))
            {
                if (waiter.WaitWithTimeout())
                {
                    return waiter.TaskToWait.GetAwaiter().GetResult();
                }
            }

            throw new InitializationTimeoutException();
        }

        private async Task<IInternalMetadata> WaitInitInternalAsync()
        {
            ValidateState();

            using (var waiter = new TaskTimeoutHelper<IInternalMetadata>(
                _initTaskCompletionSource.Task, _cluster.GetInitTimeout()))
            {
                if (await waiter.WaitWithTimeoutAsync().ConfigureAwait(false))
                {
                    return await waiter.TaskToWait.ConfigureAwait(false);
                }
            }

            throw new InitializationTimeoutException();
        }

        private void ValidateState()
        {
            var currentState = Interlocked.Read(ref _state);
            if (currentState == ClusterInitializer.Disposed)
            {
                throw new ObjectDisposedException("This cluster object has been disposed.");
            }

            if (_initException != null)
            {
                //There was an exception that is not possible to recover from
                throw _initException;
            }
        }

        public async Task ShutdownAsync(int timeoutMs = Timeout.Infinite)
        {
            var previousState = Interlocked.Exchange(ref _state, ClusterInitializer.Disposed);
            _initialized = false;

            if (previousState != ClusterInitializer.Disposed)
            {
                await _cluster.PreShutdownAsync(timeoutMs).ConfigureAwait(false);
            }
            
            if (previousState == ClusterInitializer.Initializing)
            {
                _initCancellationTokenSource.Cancel();
            }

            if (previousState != ClusterInitializer.Initialized)
            {
                try
                {
                    await _initTask.ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // ignored
                }
            }
            
            if (previousState != ClusterInitializer.Disposed)
            {
                _initCancellationTokenSource.Dispose();
            }

            if (previousState == ClusterInitializer.Initialized)
            {
                await ShutdownInternalAsync().ConfigureAwait(false);
                ClusterInitializer.Logger.Info("Cluster [" + _internalMetadata.ClusterName + "] has been shut down.");
            }
        }

        private async Task ShutdownInternalAsync()
        {
            _internalMetadata.ControlConnection.Dispose();
            await _cluster.Configuration.ProtocolEventDebouncer.ShutdownAsync().ConfigureAwait(false);
            await _cluster.PostShutdownAsync().ConfigureAwait(false);
        }
    }

    internal interface IClusterInitializer
    {
        bool TryAttachInitCallback(Func<Task> callback);

        bool IsDisposed { get; }

        void Initialize();

        Task PostInitializeAsync();

        void WaitInit();

        IInternalMetadata WaitInitAndGetMetadata();

        Task<IInternalMetadata> WaitInitAndGetMetadataAsync();

        Task WaitInitAsync();

        Task ShutdownAsync(int timeoutMs = Timeout.Infinite);
    }
}