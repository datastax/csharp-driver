namespace Cassandra
{
    /// <summary>
    ///  Policy that decides how often the reconnection to a dead node is attempted.
    ///  Each time a node is detected dead (because a connection error occurs), a new
    ///  <code>IReconnectionSchedule</code> instance is created (through the
    ///  <link>NewSchedule()</link>). Then each call to the
    ///  <link>IReconnectionSchedule#NextDelayMs</link> method of this instance will
    ///  decide when the next reconnection attempt to this node will be tried. Note
    ///  that if the driver receives a push notification from the Cassandra cluster
    ///  that a node is UP, any existing <code>IReconnectionSchedule</code> on that
    ///  node will be cancelled and a new one will be created (in effect, the driver
    ///  reset the scheduler). The default <link>ExponentialReconnectionPolicy</link>
    ///  policy is usually adequate.
    /// </summary>
    public interface IReconnectionPolicy
    {
        /// <summary>
        ///  Creates a new schedule for reconnection attempts.
        /// </summary>
        IReconnectionSchedule NewSchedule();

    }

    /// <summary>
    ///  Schedules reconnection attempts to a node.
    /// </summary>
    public interface IReconnectionSchedule
    {

        /// <summary>
        ///  When to attempt the next reconnection. This method will be called once when
        ///  the host is detected down to schedule the first reconnection attempt, and
        ///  then once after each failed reconnection attempt to schedule the next one.
        ///  Hence each call to this method are free to return a different value.
        /// </summary>
        /// 
        /// <returns>a time in milliseconds to wait before attempting the next
        ///  reconnection.</returns>
        long NextDelayMs();
    }
}
