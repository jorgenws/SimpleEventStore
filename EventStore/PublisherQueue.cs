using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace SimpleEventStore
{
    internal class PublisherQueue
    {
        BlockingCollection<TransactionTask> _publishQueue;
        IEventPublisher _publisher;

        public PublisherQueue(BlockingCollection<TransactionTask> publishQueue,
                              IEventPublisher eventPublisher)
        {
            _publishQueue = publishQueue;
            _publisher = eventPublisher;
        }

        public void Consume()
        {
            TransactionTask task;
            while (!_publishQueue.IsCompleted)
            {
                try
                {
                    task = _publishQueue.Take();
                }
                catch (InvalidOperationException)
                {
                    //this means that the the blockingcolletions is waiting on more 
                    //items while the CompleteAdding property is set
                    break;
                }

                task.IsPublished = TryPublish(task);

                task.Finish();
            }

            _publishQueue.Dispose();
            //_publisher.Dispose();
        }

        private bool TryPublish(TransactionTask transactionBatch)
        {
            bool success = false;
            int retriesLeft = 5;

            var eventTransactions = new List<EventTransaction>();

            while (!success && retriesLeft > 0)
            {
                success = _publisher.Publish(transactionBatch.Transaction);
                retriesLeft--;
            }

            return success;
        }
    }

    internal class PublisherEnqueuer
    {
        BlockingCollection<TransactionTask> _publisherQueue;

        public PublisherEnqueuer(BlockingCollection<TransactionTask> publisherQueue)
        {
            _publisherQueue = publisherQueue;
        }

        public void Enqueue(TransactionTask transactionTask)
        {
            _publisherQueue.Add(transactionTask);
        }
    }
}