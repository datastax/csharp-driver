using Cassandra;

//based on https://github.com/pchalamet/cassandra-sharp/tree/master/Samples

namespace TPLSample
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            NerdMoviesLinqSample.NerdMoviesLinqSample.Run();
            KeyspacesSample.KeyspacesSample.Run();
            FutureSample.FutureSample.Run();
            LinqKeyspacesSample.LinqKeyspacesSample.Run();

//            TimeOut.TimeOutSample.Run();
//            PreparedStatement.Batch.Run();
//            LinqSample.Run();
//            FutureSample.Run();
//            POCOSample.Run();
//            AsyncSample.Run();
        }
    }
}