//
//  Copyright (c) 2014, Kevin Thompson
//  All rights reserved.
//  
//  Redistribution and use in source and binary forms, with or without
//  modification, are permitted provided that the following conditions are met:
//  
//  1. Redistributions of source code must retain the above copyright notice, this
//     list of conditions and the following disclaimer. 
//  2. Redistributions in binary form must reproduce the above copyright notice,
//     this list of conditions and the following disclaimer in the documentation
//     and/or other materials provided with the distribution.
//  
//  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
//  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
//  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
//  DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
//  ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
//  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
//  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
//  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
//  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
//  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//

using System;
using System.Collections.Generic;
using System.Runtime.ConstrainedExecution;
using System.Runtime.InteropServices;

namespace Cassandra.DataStax.Auth.Sspi.Buffers
{
    /// <summary>
    /// Prepares SecureBuffers for providing them to native API calls.
    /// </summary>
    /// <remarks>
    /// The native APIs consume lists of buffers, with each buffer indicating its type or purpose.
    /// 
    /// The buffers themselves are simple byte arrays, and the native APIs consume arrays of buffers.
    /// 
    /// Since winapi calling convention, perhaps as an extension of C calling convention, does not
    /// provide a standard convention means of communicating the length of any array, custom structures
    /// must be created to carry the buffer length and usage.
    /// 
    /// Not only does the API need to know how long each buffer is, and how long the array of buffers is,
    /// it needs to communicate back how much of each buffer was filled; we may provide it a token buffer
    /// that is 12288 bytes long, but it might only use 125 bytes of that, which we need a way of knowing.
    /// 
    /// As a result of this, the API requires byte arrays to be carried in structs that are natively known as 
    /// SecureBuffers (known as SecureBufferInternal in this project), and then arrays of SecureBuffers are
    /// carried in a SecureBufferDescriptor structure.
    /// 
    /// As such, this class has to do a significant amount of marshaling work just to get the buffers back and
    /// forth to the native APIs.
    ///   * We have to pin all buffers
    ///   * We have to pin the array of buffers
    ///   * We have to obtain IntPtr handles to each of the buffers and to the array of buffers.
    ///   * Since we provide EasyToUse SecureBuffer classes from the rest of the project, but we 
    ///     provide SecureBufferInternal structures from the native API, we have to copy back values
    ///     from the SecureBufferInternal structs to our SecureBuffer class.
    ///     
    /// To make this class easy to use, it accepts either one or many buffers as its constructor; and
    /// implements IDisposable to know when to marshal values back from the unmanaged structures and to 
    /// release pinned handles. 
    /// 
    /// Additionally, in case the adapter is leaked without disposing, the adapter implements a Critical
    /// Finalizer, to ensure that the GCHandles are released, else we will permanently pin handles.
    /// 
    /// The typical flow is to take one or many buffers; create and fill the neccessary unmanaged structures;
    /// pin memory; acquire the IntPtr handles; let the caller access the top-level IntPtr representing
    /// the SecureBufferDescriptor, to provide to the native APIs; wait for the caller to invoke the native
    /// API; wait for the caller to invoke our Dispose; marshal back any data from the native structures
    /// (buffer write counts); release all GCHandles to unpin memory.
    /// 
    /// The total descriptor structure is as follows:
    /// |-- Descriptor handle 
    ///     |-- Array of buffers
    ///         |-- Buffer 1
    ///         |-- Buffer 2
    ///         ...
    ///         |-- Buffer N.
    ///         
    /// Each object in that structure must be pinned and passed as an IntPtr to the native APIs. 
    /// All this to pass what boils down to a List of byte arrays..
    /// </remarks>
    internal sealed class SecureBufferAdapter : CriticalFinalizerObject, IDisposable
    {
        /// <summary>
        /// Whether the adapter has already been disposed.
        /// </summary>
        private bool disposed;

        /// <summary>
        /// The list of mananged SecureBuffers the caller provided to us.
        /// </summary>
        private IList<SecureBuffer> buffers;

        /// <summary>
        /// The top level handle representing the entire descriptor.
        /// </summary>
        private GCHandle descriptorHandle;

        /// <summary>
        /// The handle representing the array of buffers.
        /// </summary>
        private GCHandle bufferCarrierHandle;

        /// <summary>
        /// The handles representing each actual buffer.
        /// </summary>
        private GCHandle[] bufferHandles;

        /// <summary>
        /// The native buffer descriptor
        /// </summary>
        private SecureBufferDescInternal descriptor;

        /// <summary>
        /// An array of the native buffers.
        /// </summary>
        private SecureBufferInternal[] bufferCarrier;

        /// <summary>
        /// Initializes a SecureBufferAdapter to carry a single buffer to the native api.
        /// </summary>
        /// <param name="buffer"></param>
        public SecureBufferAdapter( SecureBuffer buffer )
            : this( new[] { buffer } )
        {
        }

        /// <summary>
        /// Initializes the SecureBufferAdapter to carry a list of buffers to the native api.
        /// </summary>
        /// <param name="buffers"></param>
        public SecureBufferAdapter( IList<SecureBuffer> buffers ) : base()
        {
            this.buffers = buffers;

            this.disposed = false;

            this.bufferHandles = new GCHandle[this.buffers.Count];
            this.bufferCarrier = new SecureBufferInternal[this.buffers.Count];

            for ( int i = 0; i < this.buffers.Count; i++ )
            {
                this.bufferHandles[i] = GCHandle.Alloc( this.buffers[i].Buffer, GCHandleType.Pinned );

                this.bufferCarrier[i] = new SecureBufferInternal();
                this.bufferCarrier[i].Type = this.buffers[i].Type;
                this.bufferCarrier[i].Count = this.buffers[i].Buffer.Length;
                this.bufferCarrier[i].Buffer = bufferHandles[i].AddrOfPinnedObject();
            }

            this.bufferCarrierHandle = GCHandle.Alloc( bufferCarrier, GCHandleType.Pinned );

            this.descriptor = new SecureBufferDescInternal();
            this.descriptor.Version = SecureBufferDescInternal.ApiVersion;
            this.descriptor.NumBuffers = this.buffers.Count;
            this.descriptor.Buffers = bufferCarrierHandle.AddrOfPinnedObject();

            this.descriptorHandle = GCHandle.Alloc( descriptor, GCHandleType.Pinned );
        }

        [ReliabilityContract( Consistency.WillNotCorruptState, Cer.Success )]
        ~SecureBufferAdapter()
        {
            // We bend the typical Dispose pattern here. This finalizer runs in a Constrained Execution Region,
            // and so we shouldn't call virtual methods. There's no need to extend this class, so we prevent it
            // and mark the protected Dispose method as non-virtual.
            Dispose( false );
        }

        /// <summary>
        /// Gets the top-level pointer to the secure buffer descriptor to pass to the native API.
        /// </summary>
        public IntPtr Handle
        {
            get
            {
                if ( this.disposed )
                {
                    throw new ObjectDisposedException( "Cannot use SecureBufferListHandle after it has been disposed" );
                }

                return this.descriptorHandle.AddrOfPinnedObject();
            }
        }

        /// <summary>
        /// Completes any buffer passing marshaling and releases all resources associated with the adapter.
        /// </summary>
        public void Dispose()
        {
            this.Dispose( true );
            GC.SuppressFinalize( this );
        }

        /// <summary>
        /// Completes any buffer passing marshaling and releases all resources associated with the adapter.
        /// This may be called by the finalizer, or by the regular Dispose method. In the case of the finalizer,
        /// we've been leaked and there's no point in attempting to marshal back data from the native structures,
        /// nor should we anyway since they may be gone.
        /// </summary>
        /// <param name="disposing">Whether Dispose is being called.</param>
        [ReliabilityContract( Consistency.WillNotCorruptState, Cer.Success )]
        private void Dispose( bool disposing )
        {
            if ( this.disposed == true ) { return; }

            if ( disposing )
            {
                // When this class is actually being used for its original purpose - to convey buffers 
                // back and forth to SSPI calls - we need to copy the potentially modified structure members
                // back to our caller's buffer.
                for( int i = 0; i < this.buffers.Count; i++ )
                {
                    this.buffers[i].Length = this.bufferCarrier[i].Count;
                }
            }

            for( int i = 0; i < this.bufferHandles.Length; i++ )
            {
                if( this.bufferHandles[i].IsAllocated )
                {
                    this.bufferHandles[i].Free();
                }
            }

            if( this.bufferCarrierHandle.IsAllocated )
            {
                this.bufferCarrierHandle.Free();
            }

            if( this.descriptorHandle.IsAllocated )
            {
                this.descriptorHandle.Free();
            }

            this.disposed = true;
        }
    }
}
