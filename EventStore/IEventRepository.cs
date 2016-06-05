using System;
using System.Collections.Generic;

namespace SimpleEventStore
{
    public interface IEventRepository
    {
        bool WriteEvents(List<EventTransaction> eventTransaction);
        Event[] GetEventsForAggregate(Guid aggregateId);
        Event[] GetEventsForAggregate(Guid aggregateId, int largerThan);
        Event[] GetAllEvents(int from, int to);
    }
}