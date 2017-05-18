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
using System.Runtime.InteropServices;
using Dse.Auth.Sspi.Buffers;
using Dse.Auth.Sspi.Credentials;

namespace Dse.Auth.Sspi.Contexts
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
        /// The name of the authenticating authority for the context.
        /// </summary>
        public string AuthorityName
        {
            get
            {
                CheckLifecycle();
                return QueryContextString( ContextQueryAttrib.Authority );
            }
        }

        /// <summary>
        /// The logon username that the context represents.
        /// </summary>
        public string ContextUserName
        {
            get
            {
                CheckLifecycle();
                return QueryContextString( ContextQueryAttrib.Names );
            }
        }

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
            byte[] result;

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
                    WrapNoEncrypt,
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
            result = new byte[trailerBuffer.Length + dataBuffer.Length + paddingBuffer.Length];

            Array.Copy( trailerBuffer.Buffer, 0, result, position, trailerBuffer.Length );
            position += trailerBuffer.Length;

            Array.Copy( dataBuffer.Buffer, 0, result, position, dataBuffer.Length );
            position += dataBuffer.Length;

            Array.Copy( paddingBuffer.Buffer, 0, result, position, paddingBuffer.Length );

            return result;
        }

        /// <summary>
        /// Decrypts a previously encrypted message.
        /// </summary>
        /// <remarks>
        /// The expected format of the buffer is as follows:
        ///  - 2 bytes, an unsigned big-endian integer indicating the length of the trailer buffer size
        ///  - 4 bytes, an unsigned big-endian integer indicating the length of the message buffer size.
        ///  - 2 bytes, an unsigned big-endian integer indicating the length of the encryption padding buffer size.
        ///  - The trailer buffer
        ///  - The message buffer
        ///  - The padding buffer.
        /// </remarks>
        /// <param name="input">The packed and encrypted data.</param>
        /// <returns>The original plaintext message.</returns>
        public byte[] Decrypt( byte[] input )
        {
            SecPkgContext_Sizes sizes;

            SecureBuffer trailerBuffer;
            SecureBuffer dataBuffer;
            SecureBuffer paddingBuffer;
            SecureBufferAdapter adapter;

            SecurityStatus status;
            int remaining;
            int position;

            int trailerLength;
            int dataLength;
            int paddingLength;

            CheckLifecycle();

            sizes = QueryBufferSizes();

            // This check is required, but not sufficient. We could be stricter.
            if (input.Length < sizes.SecurityTrailer)
            {
                throw new ArgumentException("Buffer is too small to possibly contain an encrypted message");
            }

            position = 0;

            trailerLength = input.Length;

            dataLength = 0;

            paddingLength = 0;

            if (trailerLength + dataLength + paddingLength > input.Length)
            {
                throw new ArgumentException( "The buffer contains invalid data - the embedded length data does not add up." );
            }

            trailerBuffer = new SecureBuffer( new byte[trailerLength], BufferType.Stream );
            dataBuffer = new SecureBuffer( new byte[dataLength], BufferType.Data );
            paddingBuffer = new SecureBuffer( new byte[paddingLength], BufferType.Padding );

            remaining = input.Length - position;

            if( trailerBuffer.Length <= remaining )
            {
                Array.Copy( input, position, trailerBuffer.Buffer, 0, trailerBuffer.Length );
                position += trailerBuffer.Length;
                remaining -= trailerBuffer.Length;
            }
            else
            {
                throw new ArgumentException( "Input is missing data - it is not long enough to contain a fully encrypted message" );
            }

            if( dataBuffer.Length <= remaining )
            {
                Array.Copy( input, position, dataBuffer.Buffer, 0, dataBuffer.Length );
                position += dataBuffer.Length;
                remaining -= dataBuffer.Length;
            }
            else
            {
                throw new ArgumentException("Input is missing data - it is not long enough to contain a fully encrypted message");
            }

            if( paddingBuffer.Length <= remaining )
            {
                Array.Copy(input, position, paddingBuffer.Buffer, 0, paddingBuffer.Length);
            }
            // else there was no padding.


            using ( adapter = new SecureBufferAdapter( new[] { trailerBuffer, dataBuffer, paddingBuffer } ) )
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
                throw new SspiException( "Failed to encrypt message", status );
            }
            if (dataBuffer.Buffer.Length == 0)
            {
                // No data expected
                return null;
            }

            var result = new byte[dataBuffer.Length];
            Array.Copy(dataBuffer.Buffer, 0, result, 0, dataBuffer.Length);

            return result;
        }

        /// <summary>
        /// Signs the message using the context's session key.
        /// </summary>
        /// <remarks>
        /// The structure of the returned buffer is as follows:
        ///  - 4 bytes, unsigned big-endian integer indicating the length of the plaintext message
        ///  - 2 bytes, unsigned big-endian integer indicating the length of the signture
        ///  - The plaintext message
        ///  - The message's signature.
        /// </remarks>
        /// <param name="message"></param>
        /// <returns></returns>
        public byte[] MakeSignature( byte[] message )
        {
            SecurityStatus status = SecurityStatus.InternalError;

            SecPkgContext_Sizes sizes;
            SecureBuffer dataBuffer;
            SecureBuffer signatureBuffer;
            SecureBufferAdapter adapter;

            CheckLifecycle();

            sizes = QueryBufferSizes();

            dataBuffer = new SecureBuffer( new byte[message.Length], BufferType.Data );
            signatureBuffer = new SecureBuffer( new byte[sizes.MaxSignature], BufferType.Token );

            Array.Copy( message, dataBuffer.Buffer, message.Length );

            using ( adapter = new SecureBufferAdapter( new[] { dataBuffer, signatureBuffer } ) )
            {
                status = ContextNativeMethods.SafeMakeSignature(
                    this.ContextHandle,
                    0,
                    adapter,
                    0
                );
            }

            if ( status != SecurityStatus.OK )
            {
                throw new SspiException( "Failed to create message signature.", status );
            }

            byte[] outMessage;
            int position = 0;

            // Enough room for 
            //  - original message length (4 bytes)
            //  - signature length        (2 bytes)
            //  - original message
            //  - signature

            outMessage = new byte[4 + 2 + dataBuffer.Length + signatureBuffer.Length];

            ByteWriter.WriteInt32_BE( dataBuffer.Length, outMessage, position );
            position += 4;

            ByteWriter.WriteInt16_BE( (Int16)signatureBuffer.Length, outMessage, position );
            position += 2;

            Array.Copy( dataBuffer.Buffer, 0, outMessage, position, dataBuffer.Length );
            position += dataBuffer.Length;

            Array.Copy( signatureBuffer.Buffer, 0, outMessage, position, signatureBuffer.Length );
            position += signatureBuffer.Length;

            return outMessage;
        }

        /// <summary>
        /// Verifies the signature of a signed message
        /// </summary>
        /// <remarks>
        /// The expected structure of the signed message buffer is as follows:
        ///  - 4 bytes, unsigned integer in big endian format indicating the length of the plaintext message
        ///  - 2 bytes, unsigned integer in big endian format indicating the length of the signture
        ///  - The plaintext message
        ///  - The message's signature.
        /// </remarks>
        /// <param name="signedMessage">The packed signed message.</param>
        /// <param name="origMessage">The extracted original message.</param>
        /// <returns>True if the message has a valid signature, false otherwise.</returns>
        public bool VerifySignature( byte[] signedMessage, out byte[] origMessage )
        {
            SecurityStatus status = SecurityStatus.InternalError;

            SecPkgContext_Sizes sizes;
            SecureBuffer dataBuffer;
            SecureBuffer signatureBuffer;
            SecureBufferAdapter adapter;

            CheckLifecycle();

            sizes = QueryBufferSizes();
            
            if ( signedMessage.Length < 2 + 4 + sizes.MaxSignature )
            {
                throw new ArgumentException( "Input message is too small to possibly fit a valid message" );
            }

            int position = 0;
            int messageLen;
            int sigLen;

            messageLen = ByteWriter.ReadInt32_BE( signedMessage, 0 );
            position += 4;

            sigLen = ByteWriter.ReadInt16_BE( signedMessage, position );
            position += 2;

            if ( messageLen + sigLen + 2 + 4 > signedMessage.Length )
            {
                throw new ArgumentException( "The buffer contains invalid data - the embedded length data does not add up." );
            }

            dataBuffer = new SecureBuffer( new byte[messageLen], BufferType.Data );
            Array.Copy( signedMessage, position, dataBuffer.Buffer, 0, messageLen );
            position += messageLen;

            signatureBuffer = new SecureBuffer( new byte[sigLen], BufferType.Token );
            Array.Copy( signedMessage, position, signatureBuffer.Buffer, 0, sigLen );
            position += sigLen;

            using ( adapter = new SecureBufferAdapter( new[] { dataBuffer, signatureBuffer } ) )
            {
                status = ContextNativeMethods.SafeVerifySignature(
                    this.ContextHandle,
                    0,
                    adapter,
                    0
                );
            }

            if ( status == SecurityStatus.OK )
            {
                origMessage = dataBuffer.Buffer;
                return true;
            }
            else if ( status == SecurityStatus.MessageAltered ||
                      status == SecurityStatus.OutOfSequence )
            {
                origMessage = null;
                return false;
            }
            else
            {
                throw new SspiException( "Failed to determine the veracity of a signed message.", status );
            }
        }

        /// <summary>
        /// Queries the security package's expections regarding message/token/signature/padding buffer sizes.
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
        /// Queries a string-valued context attribute by the named attribute.
        /// </summary>
        /// <param name="attrib">The string-valued attribute to query.</param>
        /// <returns></returns>
        private string QueryContextString(ContextQueryAttrib attrib)
        {
            SecPkgContext_String stringAttrib;
            SecurityStatus status = SecurityStatus.InternalError;
            string result = null;
            bool gotRef = false;

            if( attrib != ContextQueryAttrib.Names && attrib != ContextQueryAttrib.Authority )
            {
                throw new InvalidOperationException( "QueryContextString can only be used to query context Name and Authority attributes" );
            }

            stringAttrib = new SecPkgContext_String();

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
                    status = ContextNativeMethods.QueryContextAttributes_String(
                        ref this.ContextHandle.rawHandle,
                        attrib,
                        ref stringAttrib
                    );

                    this.ContextHandle.DangerousRelease();

                    if ( status == SecurityStatus.OK )
                    {
                        result = Marshal.PtrToStringUni( stringAttrib.StringResult );
                        ContextNativeMethods.FreeContextBuffer( stringAttrib.StringResult );
                    }
                }
            }
            
            if( status == SecurityStatus.Unsupported )
            {
                return null;
            }
            else if( status != SecurityStatus.OK )
            {
                throw new SspiException( "Failed to query the context's associated user name", status );
            }

            return result;
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
