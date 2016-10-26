using SimpleEventStore;
using System;
using System.Collections.Generic;

namespace EventStoreTests.HelperClasses
{
    public class DummyEventRepository : IEventRepository
    {
        public void Dispose() { }

        public Event[] GetAllEvents(int from, int to)
        {
            throw new NotImplementedException();
        }

        public Event[] GetEventsForAggregate(Guid aggregateId)
        {
            throw new NotImplementedException();
        }

        public Event[] GetEventsForAggregate(Guid aggregateId, int largerThan)
        {
            throw new NotImplementedException();
        }

        public int NextSerialNumber()
        {
            return 0;
        }

        public void ResetSerialNumber() { }

        public bool WriteEvents(List<EventTransaction> eventTransaction)
        {
            return true;
        }
    }
}
