using System;
using System.Collections.Generic;
using System.Text;
using Moq;
using Xunit;
using SimpleEventStore;
using Events;

namespace EventStoreTests
{
    public class EventStoreTests
    {
        private readonly Guid _aggregateId = Guid.Parse("{A4067EB4-7523-4253-B788-0EE2E758E297}");

        private Mock<IEventRepository> _repository;
        private Mock<IEventPublisher> _publisher;

        [Fact]
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

        public static IEnumerable<object[]> FailingEvents
        {
            get
            {
                yield return new object[]
                {
                    new Event
                    {
                        AggregateId = Guid.Empty,
                        EventType = "some type",
                        SerializedEvent = Encoding.UTF8.GetBytes("some data")
                    }
                };

                yield return new object[]
                {
                    new Event
                    {
                        AggregateId = Guid.NewGuid(),
                        EventType = null,
                        SerializedEvent = Encoding.UTF8.GetBytes("some data")
                    }
                };
                yield return new object[]
                {
                    new Event
                    {
                        AggregateId = Guid.NewGuid(),
                        EventType = "some type",
                        SerializedEvent = null
                    }
                };
                yield return new object[]
                {
                    new Event
                    {
                        AggregateId = Guid.NewGuid(),
                        EventType = "some type",
                        SerializedEvent = new byte[0]
                    }
                };
            }
        }

        [Theory]
        [MemberData(nameof(FailingEvents))]
        public void SendEventWithMissingValuesThrowsException(Event @event)
        {
            _repository = new Mock<IEventRepository>();
            _publisher = new Mock<IEventPublisher>();
            var eventStore = new EventStore(_repository.Object, _publisher.Object);

            var transaction = new EventTransaction
            {
                Events = new[] { @event }
            };

            var task = eventStore.Process(transaction);

            Assert.IsType<InvalidOperationException>(task.Exception.InnerException);
        }
    }
}
