using Events;
using System;
using System.Collections.Generic;

namespace SimpleEventStore
{
    //Find a way to communicate that you need to serial ids in increasing order and 
    //that you must set the event to the serial id it gets persisted with
    //when implementing this interface
    public interface IEventRepository : IDisposable
    {
        bool WriteEvents(List<EventTransaction> eventTransaction);
        Event[] GetEventsForAggregate(Guid aggregateId);
        Event[] GetEventsForAggregate(Guid aggregateId, int largerThan);
        Event[] GetAllEvents(int from, int to);
        int NextSerialNumber();
        void ResetSerialNumber();
    }
}