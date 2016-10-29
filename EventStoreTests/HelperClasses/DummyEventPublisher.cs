using Events;
using SimpleEventStore;

namespace EventStoreTests.HelperClasses
{
    public class DummyEventPublisher : IEventPublisher
    {
        public bool Publish(EventTransaction eventTransaction)
        {
            return true;
        }

        public void Dispose() { }
    }
}
