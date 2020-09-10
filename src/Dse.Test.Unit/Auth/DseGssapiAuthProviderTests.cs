using System;
using Dse.Auth;
using Dse.Test.Unit.TestAttributes;
using NUnit.Framework;

namespace Dse.Test.Unit.Auth
{
    [TestFixture]
    public class DseGssapiAuthProviderTests
    {
#if NETCOREAPP && !NETCOREAPP2_0
        [WinOnly]
        [Test]
        public void When_NetStandard20AndWindows_Should_NotThrowException()
        {
            var provider = new DseGssapiAuthProvider();
        }

        [NotWindows]
        [Test]
        public void When_NetStandard20AndNotWindows_Should_ThrowException()
        {
            Assert.Throws<NotSupportedException>(() =>
            {
                var provider = new DseGssapiAuthProvider();
            });
        }
#endif

#if NET452

        [WinOnly]
        [Test]
        public void When_Net452AndWindows_Should_NotThrowException()
        {
            var provider = new DseGssapiAuthProvider();
        }

        [NotWindows]
        [Test]
        public void When_Net452AndNotWindows_Should_NotThrowException()
        {
            Assert.Throws<NotSupportedException>(() =>
            {
                var provider = new DseGssapiAuthProvider();
            });
        }

#endif
    }
}