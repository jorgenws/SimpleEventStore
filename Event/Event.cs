using System;

namespace Events
{
    public class Event
    {
        public Guid AggregateId { get; set; }
        public int SerialId { get; set; }
        public string EventType { get; set; }
        public byte[] SerializedEvent { get; set; }
    }
}
