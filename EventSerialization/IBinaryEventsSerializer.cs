using Events;

namespace EventSerialization
{
    public interface IBinaryEventsSerializer
    {
        byte[] Serialize(EventTransaction transaction);
        byte[] Serialize(Event @event);
        EventTransaction DeserializeEventTransaction(byte[] bytes);
        Event DeserializeEvent(byte[] bytes);
    }
}