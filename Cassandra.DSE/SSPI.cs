//
//      Copyright (C) 2012 DataStax Inc.
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

//based on http://www.pinvoke.net/default.aspx/secur32.initializesecuritycontext

using System;
using System.Net;

namespace SSPI
{
    internal class SSPIHelper
    {

        string _principal;
        string _hostname;
        NetworkCredential _networkCredential;
        bool _gotClientCredentials = false;

        WindowsAPI.SECURITY_HANDLE _outboundCredHandle = new WindowsAPI.SECURITY_HANDLE(0);
        WindowsAPI.SECURITY_HANDLE _clientContextHandle = new WindowsAPI.SECURITY_HANDLE(0);

        public SSPIHelper(string hostname, NetworkCredential credentials, string principal)
        {
            _hostname = hostname;
            _networkCredential = credentials;
            _principal = principal;
        }

        public void InitializeClient(out byte[] clientToken, byte[] serverToken,
                                     out bool continueProcessing)
        {
            clientToken = null;
            continueProcessing = true;

            WindowsAPI.SECURITY_INTEGER clientLifeTime = new WindowsAPI.SECURITY_INTEGER(0);

            int resultCode = -1;

            if (!_gotClientCredentials)
            {
                if (_networkCredential == null)
                {
                    resultCode = WindowsAPI.AcquireCredentialsHandle(IntPtr.Zero, "Kerberos", WindowsAPI.SECPKG_CRED_OUTBOUND,
                                                   IntPtr.Zero, IntPtr.Zero, 0, IntPtr.Zero,
                                                   ref _outboundCredHandle, ref clientLifeTime);
                }
                else
                {
                    WindowsAPI.AuthIdentityEx authIdentity = new WindowsAPI.AuthIdentityEx(_networkCredential.UserName,
                        _networkCredential.Password, _networkCredential.Domain, "!ntlm,");

                    if (_principal == null)
                    {
                        resultCode = WindowsAPI.AcquireCredentialsHandle(IntPtr.Zero, "Kerberos", WindowsAPI.SECPKG_CRED_OUTBOUND,
                                                    IntPtr.Zero, ref authIdentity, 0, IntPtr.Zero,
                                                    ref _outboundCredHandle, ref clientLifeTime);
                    }
                    else
                    {
                        resultCode = WindowsAPI.AcquireCredentialsHandle(_principal, "Kerberos", WindowsAPI.SECPKG_CRED_OUTBOUND,
                                                    IntPtr.Zero, ref authIdentity, 0, IntPtr.Zero,
                                                    ref _outboundCredHandle, ref clientLifeTime);
                    }
                }
                if (resultCode != WindowsAPI.SEC_E_OK)
                    throw WindowsAPI.CreateException(resultCode, "Couldn't acquire client credentials");

                _gotClientCredentials = true;
            }

            WindowsAPI.SecBufferDesc clientTokenBuf = new WindowsAPI.SecBufferDesc(WindowsAPI.MAX_TOKEN_SIZE);

            try
            {
                var reqContextAttributes =
                    WindowsAPI.ISC_REQ_ALLOCATE_MEMORY |
                    WindowsAPI.ISC_REQ_REPLAY_DETECT |
                    WindowsAPI.ISC_REQ_MUTUAL_AUTH |
                    WindowsAPI.ISC_REQ_IDENTIFY;

                uint contextAttributes = 0;

                WindowsAPI.SECURITY_HANDLE _retContext;

                if (serverToken == null)
                {
                    resultCode = WindowsAPI.InitializeSecurityContext(ref _outboundCredHandle,
                        IntPtr.Zero,
                        _hostname,// null string pszTargetName,
                        reqContextAttributes,
                        0,//int Reserved1,
                        WindowsAPI.SECURITY_NATIVE_DREP,//int TargetDataRep
                        IntPtr.Zero,    //Always zero first time around...
                        0, //int Reserved2,
                        out _clientContextHandle, //pHandle CtxtHandle = SecHandle
                        out clientTokenBuf,//ref SecBufferDesc pOutput, //PSecBufferDesc
                        out contextAttributes,//ref int pfContextAttr,
                        out clientLifeTime); //ref IntPtr ptsExpiry ); //PTimeStamp

                }
                else
                {
                    WindowsAPI.SecBufferDesc serverTokenBuf = new WindowsAPI.SecBufferDesc(serverToken);
                    try
                    {
                        resultCode = WindowsAPI.InitializeSecurityContext(ref _outboundCredHandle,
                            ref _clientContextHandle,
                            _hostname,// null string pszTargetName,
                            reqContextAttributes,
                            0,//int Reserved1,
                            WindowsAPI.SECURITY_NATIVE_DREP,//int TargetDataRep
                            ref serverTokenBuf,    //Always zero first time around...
                            0, //int Reserved2,
                            out _retContext, //pHandle CtxtHandle = SecHandle
                            out clientTokenBuf,//ref SecBufferDesc pOutput, //PSecBufferDesc
                            out contextAttributes,//ref int pfContextAttr,
                            out clientLifeTime); //ref IntPtr ptsExpiry ); //PTimeStamp
                    }
                    finally
                    {
                        serverTokenBuf.Dispose();
                    }

                    _clientContextHandle = _retContext;
                }

                if (resultCode != WindowsAPI.SEC_E_OK && resultCode != WindowsAPI.SEC_I_CONTINUE_NEEDED)
                {
                    throw WindowsAPI.CreateException(resultCode, "InitializeSecurityContext() failed!!!");
                }

                clientToken = clientTokenBuf.GetSecBufferByteArray();
            }
            finally
            {
                clientTokenBuf.Dispose();
            }

            continueProcessing = resultCode != WindowsAPI.SEC_E_OK;
        }

        public void Wrap(byte[] message, out byte[] encryptedBuffer)
        {
            encryptedBuffer = null;

            WindowsAPI.SECURITY_HANDLE encryptionContext = _clientContextHandle;

            WindowsAPI.SecPkgContext_Sizes contextSizes = new WindowsAPI.SecPkgContext_Sizes();

            {
                var resultCode = WindowsAPI.QueryContextAttributes(ref encryptionContext, WindowsAPI.SECPKG_ATTR_SIZES, out contextSizes);
                if (resultCode != WindowsAPI.SEC_E_OK)
                    throw WindowsAPI.CreateException(resultCode, "QueryContextAttribute() failed!!!");
            }

            WindowsAPI.MultipleSecBufferHelper[] secHelper = new WindowsAPI.MultipleSecBufferHelper[3];
            secHelper[0] = new WindowsAPI.MultipleSecBufferHelper(contextSizes.cbSecurityTrailer, WindowsAPI.SecBufferType.SECBUFFER_TOKEN);
            secHelper[1] = new WindowsAPI.MultipleSecBufferHelper(message, WindowsAPI.SecBufferType.SECBUFFER_DATA);
            secHelper[2] = new WindowsAPI.MultipleSecBufferHelper(contextSizes.cbBlockSize, WindowsAPI.SecBufferType.SECBUFFER_PADDING);

            WindowsAPI.SecBufferDesc descBuffer = new WindowsAPI.SecBufferDesc(secHelper);

            try
            {
                var resultCode = WindowsAPI.EncryptMessage(ref encryptionContext, WindowsAPI.SECQOP_WRAP_NO_ENCRYPT, ref descBuffer, 0);
                if (resultCode != WindowsAPI.SEC_E_OK)
                    throw WindowsAPI.CreateException(resultCode, "EncryptMessage() failed!!!");

                encryptedBuffer = descBuffer.GetSecBufferByteArray();
            }
            finally
            {
                descBuffer.Dispose();
            }
        }

        public void Unwrap(int messageLength, byte[] encryptedBuffer,
                                    out byte[] decryptedBuffer)
        {
            decryptedBuffer = null;

            WindowsAPI.SECURITY_HANDLE decryptionContext = _clientContextHandle;

            byte[] encryptedMessage = new byte[messageLength];
            Array.Copy(encryptedBuffer, 0, encryptedMessage, 0, messageLength);

            WindowsAPI.MultipleSecBufferHelper[] secHelper = new WindowsAPI.MultipleSecBufferHelper[2];
            secHelper[0] = new WindowsAPI.MultipleSecBufferHelper(encryptedMessage, WindowsAPI.SecBufferType.SECBUFFER_STREAM);
            secHelper[1] = new WindowsAPI.MultipleSecBufferHelper(null, WindowsAPI.SecBufferType.SECBUFFER_DATA);
            WindowsAPI.SecBufferDesc descBuffer = new WindowsAPI.SecBufferDesc(secHelper);
            try
            {
                uint encryptionQuality = 0;

                var ss = WindowsAPI.DecryptMessage(ref decryptionContext, ref descBuffer, 0, out encryptionQuality);
                if (ss != WindowsAPI.SEC_E_OK)
                    throw WindowsAPI.CreateException(ss, "DecryptMessage() failed!!!");

                decryptedBuffer = new byte[messageLength];
                Array.Copy(descBuffer.GetSecBufferByteArray(), 0, decryptedBuffer, 0, messageLength);
            }
            finally
            {
                descBuffer.Dispose();
            }
        }
    }
}