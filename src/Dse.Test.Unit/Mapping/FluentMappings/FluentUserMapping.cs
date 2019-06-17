//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using Dse.Mapping;
using Dse.Test.Unit.Mapping.Pocos;

namespace Dse.Test.Unit.Mapping.FluentMappings
{
    /// <summary>
    /// Defines how to map the FluentUser class.
    /// </summary>
    public class FluentUserMapping : Map<FluentUser>
    {
        public FluentUserMapping()
        {
            TableName("users");
            PartitionKey(u => u.Id);
            Column(u => u.Id, cm => cm.WithName("userid"));
            Column(u => u.FavoriteColor, cm => cm.WithDbType<string>());
            Column(u => u.TypeOfUser, cm => cm.WithDbType(typeof (string)));
            Column(u => u.PreferredContact, cm => cm.WithName("preferredcontactmethod").WithDbType<int>());
            Column(u => u.HairColor, cm => cm.WithDbType(typeof (int?)));
            Column(u => u.SomeIgnoredProperty, cm => cm.Ignore());
        }
    }
}
