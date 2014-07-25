using System;

namespace CqlPoco.IntegrationTests
{
    /// <summary>
    /// A user POCO with no decorations.
    /// </summary>
    public class PlainUser
    {
        public Guid UserId { get; set; }
        public string Name { get; set; }
        public int Age { get; set; }
        public DateTimeOffset CreatedDate { get; set; }
        public bool IsActive { get; set; }
    }
}