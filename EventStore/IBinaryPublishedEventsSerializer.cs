namespace SimpleEventStore
{
    public interface IBinaryPublishedEventsSerializer
    {
        byte[] Serialize(EventTransaction transaction);
    }
}
