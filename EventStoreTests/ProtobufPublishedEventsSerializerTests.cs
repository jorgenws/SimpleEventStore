using System;
using System.Collections.Generic;
using NUnit.Framework;
using SimpleEventStore;

namespace EventStoreTests
{
    [TestFixture]
    public class ProtobufPublishedEventsSerializerTests
    {
        [Test]
        public void SerializeSimpleEvent()
        {
            Guid aggreagateId = Guid.Parse("{39B880B6-1490-441C-A402-F580DC124C61}");
            ulong serialNumber = 1;
            byte[] @event = BitConverter.GetBytes(5);

            var events = new PublishedEvents
            {
                Events = new List<PublishedEvent>()
                {
                    new PublishedEvent
                    {
                        AggregateId = aggreagateId,
                        SerialNumber = serialNumber,
                        Event = @event
                    }
                }
            };

            var serializer = new ProtobufPublishedEventsSerializer();

            byte[] result = serializer.Serialize(events);

            var publisedEvents = serializer.Deserialize(result);
            var publishedEvent = publisedEvents.Events[0];

            Assert.AreEqual(aggreagateId, publishedEvent.AggregateId);
            Assert.AreEqual(serialNumber, publishedEvent.SerialNumber);
            Assert.AreEqual(@event, publishedEvent.Event);
        }

        [Test]
        public void SerializeTwoEvents()
        {
            Guid aggreagateId1 = Guid.Parse("{08B175B5-1BE8-4667-96AD-73F7F1BECA5A}");
            ulong serialNumber1 = 1;
            byte[] event1 = BitConverter.GetBytes(5);

            Guid aggreagateId2 = Guid.Parse("{E1EA3156-F646-448F-9052-D13A444A72F6}");
            ulong serialNumber2 = 2;
            byte[] event2 = BitConverter.GetBytes(7);

            var events = new PublishedEvents
            {
                Events = new List<PublishedEvent>()
                {
                    new PublishedEvent
                    {
                        AggregateId = aggreagateId1,
                        SerialNumber = serialNumber1,
                        Event = event1
                    },
                    new PublishedEvent
                    {
                        AggregateId = aggreagateId2,
                        SerialNumber = serialNumber2,
                        Event = event2
                    }
                }
            };

            var serializer = new ProtobufPublishedEventsSerializer();

            byte[] result = serializer.Serialize(events);

            var publisedEvents = serializer.Deserialize(result);
            var publishedEvent = publisedEvents.Events[0];

            Assert.AreEqual(aggreagateId1, publishedEvent.AggregateId);
            Assert.AreEqual(serialNumber1, publishedEvent.SerialNumber);
            Assert.AreEqual(event1, publishedEvent.Event);

            publishedEvent = publisedEvents.Events[1];

            Assert.AreEqual(aggreagateId2, publishedEvent.AggregateId);
            Assert.AreEqual(serialNumber2, publishedEvent.SerialNumber);
            Assert.AreEqual(event2, publishedEvent.Event);
        }
    }
}