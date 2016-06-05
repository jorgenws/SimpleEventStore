using System.Collections.Concurrent;
using System.Collections.Generic;

namespace SimpleEventStore
{
    public class EventConsumer
    {
        private readonly BlockingCollection<EventTransaction> _writerQueue;
        private IEventRepository _repository;
        private const uint MaxBatchSize = 1000;

        public EventConsumer(BlockingCollection<EventTransaction> writerQueue, IEventRepository repository)
        {
            _repository = repository;
            _writerQueue = writerQueue;
        }

        public void Consume()
        {
            List<EventTransaction> messageBatch = new List<EventTransaction>();
            EventTransaction message;
            while (true)
            {
                message = _writerQueue.Take();
                messageBatch.Add(message);

                while (messageBatch.Count <= MaxBatchSize && _writerQueue.TryTake(out message))
                    messageBatch.Add(message);

                _repository.WriteEvents(messageBatch);

                //Todo: Handle write errors
            }
        }
    }
}
