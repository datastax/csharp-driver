using System;

namespace CqlPoco.IntegrationTests.Pocos
{
    /// <summary>
    /// A user POCO with no decorations.
    /// </summary>
    public class PlainUser
    {
        // The main basic data types as properties
        public Guid UserId { get; set; }
        public string Name { get; set; }
        public int Age { get; set; }
        public DateTimeOffset CreatedDate { get; set; }
        public bool IsActive { get; set; }

        // Enum and nullable enum properties
        public UserType? TypeOfUser { get; set; }
        public ContactMethod PreferredContactMethod { get; set; }
    }
}