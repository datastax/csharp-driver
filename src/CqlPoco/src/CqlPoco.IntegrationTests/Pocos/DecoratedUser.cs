using System;

namespace CqlPoco.IntegrationTests.Pocos
{
    /// <summary>
    /// A user decorated with attributes indicating how it should be mapped.
    /// </summary>
    [TableName("users")]
    public class DecoratedUser
    {
        [Column("userid")]
        public Guid Id { get; set; }

        public string Name { get; set; }
        public int Age { get; set; }

        [Ignore]
        public int? AnUnusedProperty { get; set; }
    }
}