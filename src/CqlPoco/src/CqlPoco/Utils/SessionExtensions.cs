using System.Threading.Tasks;
using Cassandra;

namespace CqlPoco.Utils
{
    internal static class SessionExtensions
    {
        /// <summary>
        /// Wraps the BeginPrepare/EndPrepare methods with Task so it can be used with async/await.
        /// </summary>
        public static Task<PreparedStatement> PrepareAsync(this ISession session, string cqlQuery)
        {
            return Task<PreparedStatement>.Factory.FromAsync(session.BeginPrepare, session.EndPrepare, cqlQuery, null);
        }
    }
}
