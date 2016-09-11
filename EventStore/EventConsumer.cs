using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace SimpleEventStore
{
    internal class EventConsumer
    {
        private readonly BlockingCollection<TransactionTask> _writerQueue;
        private readonly IEventRepository _repository;
        private readonly IEventPublisher _publisher;
        private const uint MaxBatchSize = 10000;

        public EventConsumer(BlockingCollection<TransactionTask> writerQueue,
                             IEventRepository repository,
                             IEventPublisher publisher)
        {
            _repository = repository;
            _publisher = publisher;
            _writerQueue = writerQueue;
        }

        public void Consume()
        {
            List<TransactionTask> transactionBatch = new List<TransactionTask>();
            while (!_writerQueue.IsCompleted)
            {
                transactionBatch.Clear();
                TransactionTask message;

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

                Handle(transactionBatch);
            }

            _repository.Dispose();
            _writerQueue.Dispose();
        }

        private void Handle(List<TransactionTask> transactionBatch)
        {
            bool writeSuccess = TryWrite(transactionBatch);
            if (writeSuccess)
            {
                foreach (var transaction in transactionBatch)
                    transaction.IsWritten = true;

                bool publishSuccess = TryPublish(transactionBatch);
            }
            else
            {
                foreach (var transaction in transactionBatch)
                {
                    transaction.IsWritten = false;
                }
            }

            foreach (var transaction in transactionBatch)
                transaction.Finish();
        }

        private bool TryWrite(List<TransactionTask> transactionBatch)
        {
            bool success = false;
            int retriesLeft = 5;

            var eventTransactions = new List<EventTransaction>();
            foreach (var eventTransaction in transactionBatch)
                eventTransactions.Add(eventTransaction.Transaction);

            while (!success && retriesLeft > 0)
            {
                success = _repository.WriteEvents(eventTransactions);
                retriesLeft--;
            }

            return success;
        }

        private bool TryPublish(List<TransactionTask> transactionBatch)
        {
            bool success = false;
            int retriesLeft = 5;

            var eventTransactions = new List<EventTransaction>();
            foreach (var eventTransaction in transactionBatch)
                eventTransactions.Add(eventTransaction.Transaction);

            while (!success && retriesLeft > 0)
            {
                success = _publisher.Publish(eventTransactions);
                retriesLeft--;
            }

            return success;
        }
    }
}
