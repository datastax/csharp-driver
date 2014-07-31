using System;

namespace CqlPoco.IntegrationTests.Pocos
{
    /// <summary>
    /// A user decorated with attributes indicating how it should be mapped, specifically the ExplicitColumnsAttribute.
    /// </summary>
    [ExplicitColumns]
    public class ExplicitColumnsUser
    {
        [Column]
        public Guid UserId { get; set; }

        // Should not be mapped since no Column attribute and ExplicitColumns was used
        public string Name { get; set; }

        [Column("age")]
        public int UserAge { get; set; }
    }
}