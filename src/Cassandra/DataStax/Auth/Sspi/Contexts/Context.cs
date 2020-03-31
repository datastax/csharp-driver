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
using System.Runtime.CompilerServices;
using Cassandra.DataStax.Auth.Sspi.Buffers;
using Cassandra.DataStax.Auth.Sspi.Credentials;

namespace Cassandra.DataStax.Auth.Sspi.Contexts
{
    /// <summary>
    /// Represents a security context and provides common functionality required for all security 
    /// contexts.
    /// </summary>
    /// <remarks>
    /// This class is abstract and has a protected constructor and Initialize method. The exact 
    /// initialization implementation is provided by a subclasses, which may perform initialization 
    /// in a variety of manners.
    /// </remarks>
    internal abstract class Context : IDisposable
    {
        /// <summary>
        /// Produce a header or trailer but do not encrypt the message. See: KERB_WRAP_NO_ENCRYPT.
        /// </summary>
        private const uint WrapNoEncrypt = 0x80000001;

        /// <summary>
        /// Performs basic initialization of a new instance of the Context class.
        /// Initialization is not complete until the ContextHandle property has been set
        /// and the Initialize method has been called.
        /// </summary>
        /// <param name="cred"></param>
        protected Context( Credential cred )
        {
            this.Credential = cred;

            this.ContextHandle = new SafeContextHandle();

            this.Disposed = false;
            this.Initialized = false;
        }

        /// <summary>
        /// Whether or not the context is fully formed.
        /// </summary>
        public bool Initialized { get; private set; }

        /// <summary>
        /// The credential being used by the context to authenticate itself to other actors.
        /// </summary>
        protected Credential Credential { get; private set; }

        /// <summary>
        /// A reference to the security context's handle.
        /// </summary>
        public SafeContextHandle ContextHandle { get; private set; }
        
        /// <summary>
        /// The UTC time when the context expires.
        /// </summary>
        public DateTime Expiry { get; private set; }

        /// <summary>
        /// Whether the context has been disposed.
        /// </summary>
        public bool Disposed { get; private set; }

        /// <summary>
        /// Marks the context as having completed the initialization process, ie, exchanging of authentication tokens.
        /// </summary>
        /// <param name="expiry">The date and time that the context will expire.</param>
        protected void Initialize( DateTime expiry )
        {
            this.Expiry = expiry;
            this.Initialized = true;
        }

        /// <summary>
        /// Releases all resources associated with the context.
        /// </summary>
        public void Dispose()
        {
            Dispose( true );
            GC.SuppressFinalize( this );
        }

        /// <summary>
        /// Releases resources associated with the context.
        /// </summary>
        /// <param name="disposing">If true, release managed resources, else release only unmanaged resources.</param>
        protected virtual void Dispose( bool disposing )
        {
            if( this.Disposed ) { return; }

            if( disposing )
            {
                this.ContextHandle.Dispose();
            }

            this.Disposed = true;
        }

        /// <summary>
        /// Encrypts the byte array using the context's session key.
        /// </summary>
        /// <remarks>
        /// The structure of the returned data is as follows:
        ///  - 2 bytes, an unsigned big-endian integer indicating the length of the trailer buffer size
        ///  - 4 bytes, an unsigned big-endian integer indicating the length of the message buffer size.
        ///  - 2 bytes, an unsigned big-endian integer indicating the length of the encryption padding buffer size.
        ///  - The trailer buffer
        ///  - The message buffer
        ///  - The padding buffer.
        /// </remarks>
        /// <param name="input">The raw message to encrypt.</param>
        /// <returns>The packed and encrypted message.</returns>
        public byte[] Encrypt( byte[] input )
        {
            // The message is encrypted in place in the buffer we provide to Win32 EncryptMessage
            SecPkgContext_Sizes sizes;

            SecureBuffer trailerBuffer;
            SecureBuffer dataBuffer;
            SecureBuffer paddingBuffer;
            SecureBufferAdapter adapter;

            SecurityStatus status = SecurityStatus.InvalidHandle;
            CheckLifecycle();

            sizes = QueryBufferSizes();

            trailerBuffer = new SecureBuffer( new byte[sizes.SecurityTrailer], BufferType.Token );
            dataBuffer = new SecureBuffer( new byte[input.Length], BufferType.Data );
            paddingBuffer = new SecureBuffer( new byte[sizes.BlockSize], BufferType.Padding );

            Array.Copy( input, dataBuffer.Buffer, input.Length );

            using( adapter = new SecureBufferAdapter( new[] { trailerBuffer, dataBuffer, paddingBuffer } ) )
            {
                status = ContextNativeMethods.SafeEncryptMessage(
                    this.ContextHandle,
                    Context.WrapNoEncrypt,
                    adapter,
                    0
                );
            }

            if( status != SecurityStatus.OK )
            {
                throw new SspiException( "Failed to encrypt message", status );
            }

            int position = 0;
            
            // Return 1 buffer with the 3 buffers joined
            var result = new byte[trailerBuffer.Length + dataBuffer.Length + paddingBuffer.Length];

            Array.Copy( trailerBuffer.Buffer, 0, result, position, trailerBuffer.Length );
            position += trailerBuffer.Length;

            Array.Copy( dataBuffer.Buffer, 0, result, position, dataBuffer.Length );
            position += dataBuffer.Length;

            Array.Copy( paddingBuffer.Buffer, 0, result, position, paddingBuffer.Length );

            return result;
        }

        /// <summary>
        /// Decrypts a previously encrypted message.
        /// Assumes that the message only contains the "hash" integrity validation.
        /// </summary>
        /// <remarks>
        /// The expected format of the buffer is as follows (order is not important):
        ///  - The trailer buffer
        ///  - The message buffer
        /// </remarks>
        /// <param name="input">The packed and encrypted data.</param>
        /// <returns>Null</returns>
        /// <exception cref="SspiException">Exception thrown when hash validation fails.</exception>
        public byte[] Decrypt( byte[] input )
        {
            SecurityStatus status;

            CheckLifecycle();

            var trailerLength = input.Length;

            var dataBuffer = new SecureBuffer( new byte[0], BufferType.Data );
            var trailerBuffer = new SecureBuffer( new byte[trailerLength], BufferType.Stream );

            Array.Copy( input, 0, trailerBuffer.Buffer, 0, trailerBuffer.Length );

            using (var adapter = new SecureBufferAdapter( new[] { dataBuffer, trailerBuffer } ) )
            {
                status = ContextNativeMethods.SafeDecryptMessage(
                    this.ContextHandle,
                    0,
                    adapter,
                    0
                );
            }

            if( status != SecurityStatus.OK )
            {
                throw new SspiException( "Failed to decrypt message", status );
            }

            return null;
        }

        /// <summary>
        /// Queries the security package's expectations regarding message/token/signature/padding buffer sizes.
        /// </summary>
        /// <returns></returns>
        private SecPkgContext_Sizes QueryBufferSizes()
        {
            SecPkgContext_Sizes sizes = new SecPkgContext_Sizes();
            SecurityStatus status = SecurityStatus.InternalError;
            bool gotRef = false;

            RuntimeHelpers.PrepareConstrainedRegions();
            try
            {
                this.ContextHandle.DangerousAddRef( ref gotRef );
            }
            catch ( Exception )
            {
                if ( gotRef )
                {
                    this.ContextHandle.DangerousRelease();
                    gotRef = false;
                }

                throw;
            }
            finally
            {
                if ( gotRef )
                {
                    status = ContextNativeMethods.QueryContextAttributes_Sizes(
                        ref this.ContextHandle.rawHandle,
                        ContextQueryAttrib.Sizes,
                        ref sizes
                    );
                    this.ContextHandle.DangerousRelease();
                }
            }

            if( status != SecurityStatus.OK )
            {
                throw new SspiException( "Failed to query context buffer size attributes", status );
            }

            return sizes;
        }

        /// <summary>
        /// Verifies that the object's lifecycle (initialization / disposition) state is suitable for using the 
        /// object.
        /// </summary>
        private void CheckLifecycle()
        {
            if( this.Initialized == false )
            {
                throw new InvalidOperationException( "The context is not yet fully formed." );
            }
            else if( this.Disposed )
            {
                throw new ObjectDisposedException( "Context" );
            }
        }
    }
}
