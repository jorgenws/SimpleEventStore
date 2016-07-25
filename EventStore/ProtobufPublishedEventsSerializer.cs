using System.IO;
using ProtoBuf;
using ProtoBuf.Meta;

namespace SimpleEventStore
{
    internal class ProtobufPublishedEventsSerializer : IBinaryPublishedEventsSerializer
    {
        public ProtobufPublishedEventsSerializer()
        {
            if (!RuntimeTypeModel.Default.IsDefined(typeof(PublishedEvent)))
                RuntimeTypeModel.Default.Add(typeof(PublishedEvent), false)
                                            .Add(1, "AggregateId")
                                            .Add(2, "SerialNumber")
                                            .Add(3, "Event");
            if (!RuntimeTypeModel.Default.IsDefined(typeof(PublishedEvents)))
                RuntimeTypeModel.Default.Add(typeof(PublishedEvents), false)
                                            .Add(1, "Events");
        }
        
        public byte[] Serialize(PublishedEvents publishedEvents)
        {
            using (var ms = new MemoryStream())
            {
                Serializer.Serialize(ms, publishedEvents);
                ms.Position = 0;
                return ms.ToArray();
            }
        }

        public PublishedEvents Deserialize(byte[] bytes)
        {
            using(var ms = new MemoryStream(bytes))
            {
                ms.Position = 0;
                return Serializer.Deserialize<PublishedEvents>(ms);
            }
        }
    }
}
