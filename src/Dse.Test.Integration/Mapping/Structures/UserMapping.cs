//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using Dse.Mapping;

namespace Dse.Test.Integration.Mapping.Structures
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
