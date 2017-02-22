//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Net;

namespace Dse
{
    /// <summary>
    /// The default <c>AddressTranslater</c> used by the driver that performs no translation, returning the same IPEndPoint as the one provided.
    /// </summary>
    internal class DefaultAddressTranslator : IAddressTranslator
    {
        /// <inheritdoc />
        public IPEndPoint Translate(IPEndPoint address)
        {
            return address;
        }
    }
}