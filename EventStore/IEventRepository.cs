using System;
using System.Collections.Generic;

namespace EventStore
{
    public interface IEventRepository
    {
        bool WriteEvents(List<EventTransaction> eventTransaction);
        Event[] GetEventsForAggregate(Guid aggregateId);
    }
}
