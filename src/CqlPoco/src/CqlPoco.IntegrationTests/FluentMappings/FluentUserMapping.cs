using CqlPoco.FluentMapping;
using CqlPoco.IntegrationTests.Pocos;

namespace CqlPoco.IntegrationTests.FluentMappings
{
    /// <summary>
    /// Defines how to map the FluentUser class.
    /// </summary>
    public class FluentUserMapping : Map<FluentUser>
    {
        public FluentUserMapping()
        {
            TableName("users");
            PrimaryKey(u => u.Id);
            Column(u => u.Id, cm => cm.WithName("userid"));
            Column(u => u.FavoriteColor, cm => cm.WithDbType<string>());
            Column(u => u.TypeOfUser, cm => cm.WithDbType(typeof (string)));
            Column(u => u.PreferredContact, cm => cm.WithName("preferredcontactmethod").WithDbType<int>());
            Column(u => u.HairColor, cm => cm.WithDbType(typeof (int?)));
            Column(u => u.SomeIgnoredProperty, cm => cm.Ignore());
        }
    }
}
