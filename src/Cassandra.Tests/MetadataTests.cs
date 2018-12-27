namespace Cassandra.Tests
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    using Moq;

    using NUnit.Framework;

    [TestFixture]
    public class MetadataTests
    {
        [Test]
        public void Should_ThrowOperationCanceledException_When_CancelTokenIsCanceled()
        {
            var metadata = new Metadata(new Configuration());
            var ccMock = new Mock<IMetadataQueryProvider>(MockBehavior.Strict);
            metadata.ControlConnection = ccMock.Object;
            ccMock.Setup(c => c.QueryAsync(It.IsAny<string>(), It.IsAny<bool>())).Returns(async () =>
            {
                await Task.Delay(5000).ConfigureAwait(false);
                return null;
            });
            var cts = new CancellationTokenSource();
            cts.CancelAfter(TimeSpan.FromMilliseconds(5));

            // Catch instead of Throws because the actual thrown exception may be TaskCancelledException
            var ex = Assert.CatchAsync<OperationCanceledException>(
                () => metadata.CheckSchemaAgreementAsync(cts.Token));

            Assert.IsNotNull(ex);
        }
    }
}