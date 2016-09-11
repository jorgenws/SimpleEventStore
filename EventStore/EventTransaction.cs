using System;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleEventStore
{
    public class EventTransaction
    {
        public Guid AggregateId { get; set; }
        public Event[] Events { get; set; }
    }
}
