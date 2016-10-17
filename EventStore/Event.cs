using System;

namespace SimpleEventStore
{
    public class Event
    {
        public Guid AggregateId { get; set; }
        public int SerialId { get; set; }
        public byte[] SerializedEvent { get; set; }
    }
}
