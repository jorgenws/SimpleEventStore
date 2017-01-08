using System.IO;
using ProtoBuf;
using ProtoBuf.Meta;
using Events;

namespace EventSerialization
{
    public class ProtobufEventsSerializer : IBinaryEventsSerializer
    {
        public ProtobufEventsSerializer()
        {
            if (!RuntimeTypeModel.Default.IsDefined(typeof(Event)))
                RuntimeTypeModel.Default.Add(typeof(Event), false)
                                            .Add(1, "AggregateId")
                                            .Add(2, "SerialId")
                                            .Add(3, "EventType")
                                            .Add(4, "SerializedEvent");
            if (!RuntimeTypeModel.Default.IsDefined(typeof(EventTransaction)))
                RuntimeTypeModel.Default.Add(typeof(EventTransaction), false)
                                            .Add(1, "Events");
        }
        
        public byte[] Serialize(EventTransaction eventTransaction)
        {
            return Serialize<EventTransaction>(eventTransaction);
        }

        public byte[] Serialize(Event @event)
        {
            return Serialize<Event>(@event);
        }

        private byte[] Serialize<T>(T item)
        {
            using (var ms = new MemoryStream())
            {
                Serializer.Serialize(ms, item);
                return ms.ToArray();
            }
        }

        public EventTransaction DeserializeEventTransaction(byte[] bytes)
        {
            return Deserialize<EventTransaction>(bytes);
        }

        public Event DeserializeEvent(byte[] bytes)
        {
            return Deserialize<Event>(bytes);
        }

        private T Deserialize<T>(byte[] bytes)
        {
            using(var ms = new MemoryStream(bytes))
            {
                ms.Position = 0;
                return Serializer.Deserialize<T>(ms);
            }
        }
    }
}
