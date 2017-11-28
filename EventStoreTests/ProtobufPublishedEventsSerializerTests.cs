using System;
using Xunit;
using Events;
using EventSerialization;

namespace EventStoreTests
{
    public class ProtobufPublishedEventsSerializerTests
    {
        [Fact]
        public void SerializeSimpleEvent()
        {
            Guid aggreagateId = Guid.Parse("{39B880B6-1490-441C-A402-F580DC124C61}");
            int serialNumber = 1;
            byte[] @event = BitConverter.GetBytes(5);

            var events = new EventTransaction
            {
                Events = new []
                {
                    new Event
                    {
                        AggregateId = aggreagateId,
                        SerialId = serialNumber,
                        SerializedEvent = @event
                    }
                }
            };

            var serializer = new ProtobufEventsSerializer();

            byte[] result = serializer.Serialize(events);

            var publishedEvents = serializer.DeserializeEventTransaction(result);
            var publishedEvent = publishedEvents.Events[0];

            Assert.Equal(aggreagateId, publishedEvents.Events[0].AggregateId);
            Assert.Equal(serialNumber, publishedEvent.SerialId);
            Assert.Equal(@event, publishedEvent.SerializedEvent);
        }

        [Fact]
        public void SerializeTwoEvents()
        {
            Guid aggreagateId1 = Guid.Parse("{08B175B5-1BE8-4667-96AD-73F7F1BECA5A}");
            int serialNumber1 = 1;
            int serialNumber2 = 2;
            byte[] event1 = BitConverter.GetBytes(5);
            byte[] event2 = BitConverter.GetBytes(7);

            var events = new EventTransaction
            {
                Events = new[]
                {
                    new Event
                    {
                        AggregateId = aggreagateId1,
                        SerialId = serialNumber1,
                        SerializedEvent = event1
                    },
                    new Event
                    {
                        AggregateId = aggreagateId1,
                        SerialId = serialNumber2,
                        SerializedEvent = event2
                    }
                }
            };

            var serializer = new ProtobufEventsSerializer();

            byte[] result = serializer.Serialize(events);

            var publishedEvents = serializer.DeserializeEventTransaction(result);
            var publishedEvent = publishedEvents.Events[0];

            Assert.Equal(aggreagateId1, publishedEvents.Events[0].AggregateId);
            Assert.Equal(serialNumber1, publishedEvent.SerialId);
            Assert.Equal(event1, publishedEvent.SerializedEvent);

            publishedEvent = publishedEvents.Events[1];

            Assert.Equal(serialNumber2, publishedEvent.SerialId);
            Assert.Equal(event2, publishedEvent.SerializedEvent);
        }
    }
}