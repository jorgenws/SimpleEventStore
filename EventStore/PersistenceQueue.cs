using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace SimpleEventStore
{
    internal class PersistenceQueue
    {
        private readonly BlockingCollection<TransactionTask> _writerQueue;
        private readonly PublisherEnqueuer _publisherEnqueuer;
        private readonly IEventRepository _repository;
        private const uint MaxBatchSize = 100000;

        public PersistenceQueue(BlockingCollection<TransactionTask> writerQueue,
                                PublisherEnqueuer publisherEnqueuer,
                                IEventRepository repository)
        {
            _repository = repository;
            _publisherEnqueuer = publisherEnqueuer;
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

                var writeSuccess = TryWrite(transactionBatch);
                if (writeSuccess)
                {
                    foreach (var transaction in transactionBatch)
                    {
                        transaction.IsWritten = true;
                        _publisherEnqueuer.Enqueue(transaction);
                    }
                        
                }
            }

            _repository.Dispose();
            _writerQueue.Dispose();
        }

        private bool TryWrite(List<TransactionTask> transactionBatch)
        {
            bool success = false;
            int retriesLeft = 5;

            SetSerialNumber(transactionBatch);

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

        private void SetSerialNumber(List<TransactionTask> transactionBatch)
        {
            foreach (var transaction in transactionBatch)
                foreach (var @event in transaction.Transaction.Events)
                    @event.SerialId = _repository.NextSerialNumber();
        }
    }
}