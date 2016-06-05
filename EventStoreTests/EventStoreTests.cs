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
        public void SendEventsToConsumerGetsSentToRepository()
        {
            _repository = new Mock<IEventRepository>();
            _repository.Setup(c => c.WriteEvents(It.IsAny<List<EventTransaction>>()))
                       .Returns(() => true);
            _publisher = new Mock<IEventPublisher>();
            _publisher.Setup(c => c.Publish(It.IsAny<List<EventTransaction>>()))
                      .Returns(() => true);

            var eventStore = new EventStore(_repository.Object, _publisher.Object);

            var eventTransaction = new EventTransaction
            {
                AggregateId = _aggregateId,
                Events = new[] {new Event {SerializedEvent = Encoding.UTF8.GetBytes("some data")}}
            };

            eventStore.Process(eventTransaction).Wait();

            _repository.Verify(c => c.WriteEvents(It.IsAny<List<EventTransaction>>()), Times.AtLeastOnce());

            eventStore.Dispose();
        }
    }
}
