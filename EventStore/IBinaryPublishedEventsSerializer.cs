using System;
using System.Collections.Generic;

namespace SimpleEventStore
{
    public interface IBinaryPublishedEventsSerializer
    {
        byte[] Serialize(PublishedEvents transaction);
    }

    public class PublishedEvents
    {
        public List<PublishedEvent> Events { get; set; }
    }

    public class PublishedEvent
    {
        public Guid AggregateId { get; set; }
        public ulong SerialNumber { get; set; }
        public byte[] Event { get; set; }
    }
}
