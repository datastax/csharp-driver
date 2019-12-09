namespace Cassandra.IntegrationTests.SimulacronAPI
{
    public class PrimeRequest : IPrimeRequest
    {
        private readonly IWhen _when;
        private readonly IThen _then;

        public PrimeRequest(IWhen @when, IThen then)
        {
            _when = when;
            _then = then;
        }

        public object Render()
        {
            return new
            {
                when = _when.Render(),
                then = _then.Render()
            };
        }
    }
}