using System;
using System.Collections.Generic;
using System.Text;
using Moq;
using NUnit.Framework;
using SimpleEventStore;

namespace EventStoreTests
{
    [TestFixture]
    public class EventStoreTests
    {
        private readonly Guid _aggregateId = Guid.Parse("{A4067EB4-7523-4253-B788-0EE2E758E297}");

        private Mock<IEventRepository> _repository;
        private Mock<IEventPublisher> _publisher;

        [Test]
        public void SendEventsToConsumerGetsSentToRepositoryAndPublisher()
        {
            _repository = new Mock<IEventRepository>();
            _repository.Setup(c => c.WriteEvents(It.IsAny<List<EventTransaction>>()))
                       .Returns(() => true);
            _publisher = new Mock<IEventPublisher>();
            _publisher.Setup(c => c.Publish(It.IsAny<EventTransaction>()))
                      .Returns(() => true);

            var eventStore = new EventStore(_repository.Object, _publisher.Object);

            var eventTransaction = new EventTransaction
            {
                Events = new[] {
                    new Event {
                    AggregateId = _aggregateId,
                    SerializedEvent = Encoding.UTF8.GetBytes("some data"),
                    EventType = "Event type"
                    }
                }
            };

            eventStore.Process(eventTransaction).Wait();

            _repository.Verify(c => c.WriteEvents(It.IsAny<List<EventTransaction>>()), Times.AtLeastOnce());
            _publisher.Verify(c => c.Publish(It.IsAny<EventTransaction>()), Times.AtLeastOnce());

            eventStore.Dispose();
        }

        static IEnumerable<Event> FailingEvents()
        {
            yield return new Event
            {
                AggregateId = Guid.Empty,
                EventType = "some type",
                SerializedEvent = Encoding.UTF8.GetBytes("some data")
            };
            yield return new Event
            {
                AggregateId = Guid.NewGuid(),
                EventType = null,
                SerializedEvent = Encoding.UTF8.GetBytes("some data")
            };
            yield return new Event
            {
                AggregateId = Guid.NewGuid(),
                EventType = "some type",
                SerializedEvent = null
            };
            yield return new Event
            {
                AggregateId = Guid.NewGuid(),
                EventType = "some type",
                SerializedEvent = new byte[0]
            };
        }

        [TestCaseSource(nameof(FailingEvents))]
        public void SendEventWithMissingValuesThrowsException(Event @event)
        {
            _repository = new Mock<IEventRepository>();
            _publisher = new Mock<IEventPublisher>();
            var eventStore = new EventStore(_repository.Object, _publisher.Object);

            var transaction = new EventTransaction
            {
                Events = new[] { @event }
            };

            Assert.Throws<InvalidOperationException>(()=>eventStore.Process(transaction));
        }
    }
}
