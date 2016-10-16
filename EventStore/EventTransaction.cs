using System;

namespace SimpleEventStore
{
    public class EventTransaction
    {
        public Guid AggregateId { get; set; }
        public Event[] Events { get; set; }
    }
}
