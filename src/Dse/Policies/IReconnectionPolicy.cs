//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse
{
    /// <summary>
    ///  Policy that decides how often the reconnection to a dead node is attempted.
    ///  Each time a node is detected dead (because a connection error occurs), a new
    ///  <c>IReconnectionSchedule</c> instance is created (through the
    ///  <link>NewSchedule()</link>). Then each call to the
    ///  <link>IReconnectionSchedule#NextDelayMs</link> method of this instance will
    ///  decide when the next reconnection attempt to this node will be tried. Note
    ///  that if the driver receives a push notification from the Cassandra cluster
    ///  that a node is UP, any existing <c>IReconnectionSchedule</c> on that
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
}
