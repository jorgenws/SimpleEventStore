using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace SimpleEventStore
{
    internal class EventConsumer
    {
        private readonly BlockingCollection<EventTransaction> _writerQueue;
        private readonly IEventRepository _repository;
        private readonly IEventPublisher _publisher;
        private const uint MaxBatchSize = 1000;

        public EventConsumer(BlockingCollection<EventTransaction> writerQueue,
                             IEventRepository repository,
                             IEventPublisher publisher)
        {
            _repository = repository;
            _publisher = publisher;
            _writerQueue = writerQueue;
        }

        public void Consume()
        {
            List<EventTransaction> transactionBatch = new List<EventTransaction>();
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

                transactionBatch.Add(message);

                while (transactionBatch.Count <= MaxBatchSize && _writerQueue.TryTake(out message))
                    transactionBatch.Add(message);

                bool writeSuccess = false;
                bool publishSuccess = false;
                writeSuccess = TryWrite(transactionBatch);
                if (writeSuccess)
                    publishSuccess = TryPublish(transactionBatch);

                if (writeSuccess)
                    foreach (var transaction in transactionBatch)
                        transaction.Finished(writeSuccess, publishSuccess);
            }

            _writerQueue.Dispose();
        }

        private bool TryWrite(List<EventTransaction> transactionBatch)
        {
            bool success = false;
            int retriesLeft = 5;

            while (!success && retriesLeft > 0)
            {
                success = _repository.WriteEvents(transactionBatch);
                retriesLeft--;
            }

            return success;
        }

        private bool TryPublish(List<EventTransaction> transactionBatch)
        {
            bool success = false;
            int retriesLeft = 5;
            while (!success && retriesLeft > 0)
            {
                success = _publisher.Publish(transactionBatch);
                retriesLeft--;
            }

            return success;
        }
    }
}
