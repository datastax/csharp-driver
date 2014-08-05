namespace CqlPoco.IntegrationTests.Pocos
{
    /// <summary>
    /// A decorated POCO for use when testing UDPATE/DELETE.
    /// </summary>
    [PrimaryKey("userid")]
    public class UserWithPrimaryKeyDecoration : InsertUser
    {
    }
}