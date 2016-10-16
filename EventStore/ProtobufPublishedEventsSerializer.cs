using System.IO;
using ProtoBuf;
using ProtoBuf.Meta;

namespace SimpleEventStore
{
    internal class ProtobufPublishedEventsSerializer : IBinaryPublishedEventsSerializer
    {
        public ProtobufPublishedEventsSerializer()
        {
            if (!RuntimeTypeModel.Default.IsDefined(typeof(Event)))
                RuntimeTypeModel.Default.Add(typeof(Event), false)
                                            .Add(1, "SerialId")
                                            .Add(2, "SerializedEvent");
            if (!RuntimeTypeModel.Default.IsDefined(typeof(EventTransaction)))
                RuntimeTypeModel.Default.Add(typeof(EventTransaction), false)
                                            .Add(1, "AggregateId")
                                            .Add(2, "Events");
        }
        
        public byte[] Serialize(EventTransaction eventTransaction)
        {
            using (var ms = new MemoryStream())
            {
                Serializer.Serialize(ms, eventTransaction);
                ms.Position = 0;
                return ms.ToArray();
            }
        }

        public EventTransaction Deserialize(byte[] bytes)
        {
            using(var ms = new MemoryStream(bytes))
            {
                ms.Position = 0;
                return Serializer.Deserialize<EventTransaction>(ms);
            }
        }
    }
}
