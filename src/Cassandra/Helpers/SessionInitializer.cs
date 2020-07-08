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
using System.Threading;
using System.Threading.Tasks;

using Cassandra.Connections.Control;
using Cassandra.Requests;
using Cassandra.SessionManagement;
using Cassandra.Tasks;

namespace Cassandra.Helpers
{
    internal class SessionInitializer : ISessionInitializer
    {
        private const int Disposed = 10;
        private const int Initialized = 5;
        private const int Initializing = 1;

        private static readonly Logger Logger = new Logger(typeof(SessionInitializer));

        private readonly IInternalSession _session;
        private readonly CancellationTokenSource _initCancellationTokenSource = new CancellationTokenSource();

        private volatile bool _initialized = false;
        private volatile Task _initTask;

        private volatile TaskCompletionSource<IInternalMetadata> _initTaskCompletionSource
            = new TaskCompletionSource<IInternalMetadata>();

        private volatile InitFatalErrorException _initException;

        private volatile bool _addedCallback = true;

        private long _state = SessionInitializer.Initializing;

        public SessionInitializer(IInternalSession session)
        {
            _session = session;
        }

        public bool IsInitialized => _initialized;

        public void Initialize()
        {
            if (_session.InternalCluster.ClusterInitializer.TryAttachInitCallback(this))
            {
                _addedCallback = true;
                return;
            }

            if (_session.InternalCluster.ClusterInitializer.IsDisposed)
            {
                return;
            }

            _initTask = Task.Run(InitInternalAsync);
        }

        public Task ClusterInitCallbackAsync()
        {
            return InitInternalAsync();
        }

        public bool IsDisposed => Interlocked.Read(ref _state) == SessionInitializer.Disposed;

        private async Task InitInternalAsync()
        {
            _addedCallback = false;

            SessionInitializer.Logger.Info("Connecting to session [{0}]", _session.SessionName);
            try
            {
                if (IsDisposed)
                {
                    throw new ObjectDisposedException("Session instance was disposed before initialization finished.");
                }

                await _session.PostInitializeAsync().ConfigureAwait(false);

                var previousState = Interlocked.CompareExchange(ref _state, SessionInitializer.Initialized, SessionInitializer.Initializing);
                if (previousState == SessionInitializer.Disposed)
                {
                    await _session.OnShutdownAsync().ConfigureAwait(false);
                    throw new ObjectDisposedException("Session instance was disposed before initialization finished.");
                }

                _initialized = true;

                SessionInitializer.Logger.Info("Session [{0}] has been initialized.", _session.SessionName);

                Task.Run(() => _initTaskCompletionSource.TrySetResult(_session.InternalCluster.InternalMetadata)).Forget();
                return;
            }
            catch (Exception ex)
            {
                if (IsDisposed)
                {
                    var newEx = new ObjectDisposedException("Session instance was disposed before initialization finished.");
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
                return _session.InternalCluster.InternalMetadata;
            }

            return WaitInitInternal();
        }

        public Task<IInternalMetadata> WaitInitAndGetMetadataAsync()
        {
            if (_initialized)
            {
                //It was already initialized
                return Task.FromResult(_session.InternalCluster.InternalMetadata);
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
            var waiter = new TaskTimeoutHelper<IInternalMetadata>(
                new[]
                {
                    _session.InternalCluster.ClusterInitializer.WaitInitAndGetMetadataAsync(),
                    _initTaskCompletionSource.Task
                });

            if (waiter.WaitWithTimeout(_session.InternalCluster.GetInitTimeout()))
            {
                return waiter.TaskToWait.GetAwaiter().GetResult();
            }

            throw new InitializationTimeoutException();
        }

        private async Task<IInternalMetadata> WaitInitInternalAsync()
        {
            ValidateState();
            
            var waiter = new TaskTimeoutHelper<IInternalMetadata>(
                new[]
                {
                    _session.InternalCluster.ClusterInitializer.WaitInitAndGetMetadataAsync(),
                    _initTaskCompletionSource.Task
                });

            if (await waiter.WaitWithTimeoutAsync(_session.InternalCluster.GetInitTimeout()).ConfigureAwait(false))
            {
                return await waiter.TaskToWait.ConfigureAwait(false);
            }

            throw new InitializationTimeoutException();
        }

        private void ValidateState()
        {
            var currentState = Interlocked.Read(ref _state);
            if (currentState == SessionInitializer.Disposed)
            {
                throw new ObjectDisposedException("This session object has been disposed.");
            }

            if (_initException != null)
            {
                //There was an exception that is not possible to recover from
                throw _initException;
            }
        }

        public async Task ShutdownAsync(int timeoutMs = Timeout.Infinite)
        {
            var previousState = Interlocked.Exchange(ref _state, SessionInitializer.Disposed);
            _initialized = false;

            if (previousState == SessionInitializer.Initializing)
            {
                _initCancellationTokenSource.Cancel();
            }
            
            try
            {
                if (_initTask != null)
                {
                    await _initTask.ConfigureAwait(false);
                }
            }
            catch (Exception)
            {
                // ignored
            }

            _initialized = false;

            if (_addedCallback)
            {
                _session.InternalCluster.ClusterInitializer.RemoveCallback(this);
            }

            if (previousState != SessionInitializer.Disposed)
            {
                _initCancellationTokenSource.Dispose();
                await _session.InternalCluster.OnSessionShutdownAsync(_session).ConfigureAwait(false);
            }

            if (previousState == SessionInitializer.Initialized)
            {
                await _session.OnShutdownAsync().ConfigureAwait(false);
                SessionInitializer.Logger.Info("Session [{0}] has been shut down.", _session.SessionName);
            }
        }
    }

    internal interface ISessionInitializer
    {
        bool IsDisposed { get; }
        bool IsInitialized { get; }

        void Initialize();

        void WaitInit();

        IInternalMetadata WaitInitAndGetMetadata();

        Task<IInternalMetadata> WaitInitAndGetMetadataAsync();

        Task WaitInitAsync();

        Task ShutdownAsync(int timeoutMs = Timeout.Infinite);

        Task ClusterInitCallbackAsync();
    }
}