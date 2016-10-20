using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace SimpleEventStore
{
    internal class PublisherQueue
    {
        BlockingCollection<EventTransaction> _publishQueue;
        IEventPublisher _publisher;

        public PublisherQueue(BlockingCollection<EventTransaction> publishQueue,
                              IEventPublisher eventPublisher)
        {
            _publishQueue = publishQueue;
            _publisher = eventPublisher;
        }

        public void Consume()
        {
            EventTransaction transaction;
            while (!_publishQueue.IsCompleted)
            {
                try
                {
                    transaction = _publishQueue.Take();
                }
                catch (InvalidOperationException)
                {
                    //this means that the the blockingcolletions is waiting on more 
                    //items while the CompleteAdding property is set
                    break;
                }

                TryPublish(transaction);

            }

            _publishQueue.Dispose();
        }

        private bool TryPublish(EventTransaction transaction)
        {
            bool success = false;
            int retriesLeft = 5;

            var eventTransactions = new List<EventTransaction>();

            while (!success && retriesLeft > 0)
            {
                success = _publisher.Publish(transaction);
                retriesLeft--;
            }

            return success;
        }
    }

    internal class PublisherEnqueuer
    {
        BlockingCollection<EventTransaction> _publisherQueue;

        public PublisherEnqueuer(BlockingCollection<EventTransaction> publisherQueue)
        {
            _publisherQueue = publisherQueue;
        }

        public void Enqueue(EventTransaction transaction)
        {
            _publisherQueue.Add(transaction);
        }
    }
}