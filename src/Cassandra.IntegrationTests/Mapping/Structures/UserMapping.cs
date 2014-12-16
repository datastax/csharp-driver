using System;
using Cassandra.Mapping;

namespace Cassandra.IntegrationTests.Mapping.Structures
{
    /// <summary>
    /// Defines how to map the User class.
    /// </summary>
    public class UserMapping : Map<User>
    {
        public UserMapping()
        {
            TableName("users");
            PartitionKey(u => u.Id);
            Column(u => u.HairColor, cm => cm.WithDbType<string>()).CaseSensitive();
            Column(u => u.Id, cm => cm.WithName("Id"));
            Column(u => u.IsActive, cm => cm.WithName("IsActive"));
            Column(u => u.PreferredContactMethod, cm => cm.WithName("PreferredContactMethod").WithDbType<string>().WithName("PreferredContactMethod"));
            Column(u => u.SomeIgnoredProperty, cm => cm.Ignore());
            Column(u => u.TypeOfUser, cm => cm.WithDbType<int>().WithName("TypeOfUser"));
            Column(u => u.LastLoginDate, cm => cm.WithName("LastLoginDate"));
        }
    }
}
