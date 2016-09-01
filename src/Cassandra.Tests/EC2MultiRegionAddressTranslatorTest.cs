using System;
using Moq;
using NUnit.Framework;
using System.Net;
using System.Net.Sockets;

namespace Cassandra.Tests
{
    [TestFixture]
    public class EC2MultiRegionAddressTranslatorTest
    {
        public EC2MultiRegionAddressTranslatorTest()
        {
            Diagnostics.CassandraTraceSwitch.Level = System.Diagnostics.TraceLevel.Info;
        }

        [Test]
        public void ShouldReturnSameAddressWhenNoEntryFound()
        {
            Mock<IDnsWrapper> dnsMock = new Mock<IDnsWrapper>();
            //System.Net.Dns throws SocketException if no hostname is found for address
            dnsMock.Setup(d => d.GetHostEntry(It.IsAny<IPAddress>())).Throws(new SocketException());
            EC2MultiRegionAddressTranslator translator = new EC2MultiRegionAddressTranslator(dnsMock.Object);
            IPEndPoint endpoint = new IPEndPoint(IPAddress.Parse("10.0.0.2"), 9042);
            Assert.AreEqual(translator.Translate(endpoint), endpoint);
        }

        [Test]
        public void ShouldReturnSameAddressWhenExceptionEncountered()
        {
            Mock<IDnsWrapper> dnsMock = new Mock<IDnsWrapper>();
            dnsMock.Setup(d => d.GetHostEntry(It.IsAny<IPAddress>())).Throws(new Exception());
            EC2MultiRegionAddressTranslator translator = new EC2MultiRegionAddressTranslator(dnsMock.Object);
            IPEndPoint endpoint = new IPEndPoint(IPAddress.Parse("10.0.0.2"), 9042);
            Assert.AreEqual(translator.Translate(endpoint), endpoint);
        }

        [Test]
        public void ShouldReturnNewAddressWhenMatchFound()
        {
            IPEndPoint expectedEndpoint = new IPEndPoint(IPAddress.Parse("10.0.0.2"), 9042);

            Mock<IDnsWrapper> dnsMock = new Mock<IDnsWrapper>();
            dnsMock.Setup(d => d.GetHostEntry(It.IsAny<IPAddress>()))
                .Returns(() => new IPHostEntry() { HostName = "ec2-publicaddress.amazonaws.com" });
            dnsMock.Setup(d => d.GetHostEntry(It.IsAny<string>()))
                .Returns(() => new IPHostEntry()
                {
                    HostName = "ec2-publicaddress.amazonaws.com",
                    AddressList = new IPAddress[] { IPAddress.Parse("10.0.0.2") }
                });
            EC2MultiRegionAddressTranslator translator = new EC2MultiRegionAddressTranslator(dnsMock.Object);
            IPEndPoint publicEndpoint = new IPEndPoint(IPAddress.Parse("56.12.34.56"), 9042);
            Assert.AreEqual(translator.Translate(publicEndpoint), expectedEndpoint);
        }
    }
}
