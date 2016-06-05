using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace SimpleEventStore
{
    internal class EventConsumer
    {
        private readonly BlockingCollection<EventTransaction> _writerQueue;
        private readonly IEventRepository _repository;
        private const uint MaxBatchSize = 1000;

        public EventConsumer(BlockingCollection<EventTransaction> writerQueue, IEventRepository repository)
        {
            _repository = repository;
            _writerQueue = writerQueue;
        }

        public void Consume()
        {
            List<EventTransaction> messageBatch = new List<EventTransaction>();
            while (!_writerQueue.IsCompleted)
            {
                EventTransaction message;

                try
                {
                    message = _writerQueue.Take();
                }
                catch (InvalidOperationException)
                {
                    //this means that the the blockingcolletions is waiting on more 
                    //items while the CompleteAdding property is set
                    break;
                }

                messageBatch.Add(message);

                while (messageBatch.Count <= MaxBatchSize && _writerQueue.TryTake(out message))
                    messageBatch.Add(message);

                //TOdo: publish to external queues
                if (_repository.WriteEvents(messageBatch))
                    foreach (var transaction in messageBatch)
                        transaction.HasFinished();

                //Todo: Handle write errors

            }
        }
    }
}
