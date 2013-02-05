using Cassandra;


namespace TPLSample
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            FutureSample.FutureSample.Run();
            LinqKeyspacesSample.LinqKeyspacesSample.Run();
            KeyspacesSample.KeyspacesSample.Run();
            NerdMoviesLinqSample.NerdMoviesLinqSample.Run();

//            TimeOut.TimeOutSample.Run();
//            PreparedStatement.Batch.Run();
//            LinqSample.Run();
//            FutureSample.Run();
//            POCOSample.Run();
//            AsyncSample.Run();
        }
    }
}