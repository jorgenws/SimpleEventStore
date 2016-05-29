using System;

namespace EventStore
{
    public class Event
    {
        public Guid AggregateId { get; set; }
        public byte[] SerializedEvent { get; set; }
    }
}
