namespace SimpleEventStore
{
    public class Event
    {
        public int SerialId { get; set; }
        public byte[] SerializedEvent { get; set; }
    }
}
