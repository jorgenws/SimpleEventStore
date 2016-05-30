using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace EventStore
{
    public class EventStore
    {
        private readonly BlockingCollection<EventTransaction> _writerQueue;
        private Task _writerRunner;

        private const int BufferSize = 100000;

        public EventStore(IEventRepository repository)
        {
            _writerQueue = new BlockingCollection<EventTransaction>(BufferSize);
            
            //ToDo: Look into using continuation to catch that the task died and recreate it if possible.
            //ToDo: Add cancelation
            _writerRunner = Task.Factory.StartNew(() => new EventConsumer(_writerQueue, repository).Consume(),
                                                        TaskCreationOptions.LongRunning);
        }

        public async Task Process(EventTransaction eventTransaction)
        {
            _writerQueue.Add(eventTransaction);
            await eventTransaction.WaitAsync();
        }
    }
}
