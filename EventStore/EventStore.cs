using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace SimpleEventStore
{
    public class EventStore : IDisposable
    {
        private readonly BlockingCollection<TransactionTask> _writerQueue;
        private readonly IEventRepository _eventRepository;
        private readonly IEventPublisher _publisher;
        private Task _writerRunner;

        private const int BufferSize = 100000;

        public EventStore(IEventRepository repository,
                          IEventPublisher publisher)
        {
            _eventRepository = repository;
            _publisher = publisher;
            _writerQueue = new BlockingCollection<TransactionTask>(BufferSize);
            
            //ToDo: Look into using continuation to catch that the task died and recreate it if possible.
            _writerRunner = Task.Factory.StartNew(() => new EventConsumer(_writerQueue, repository, _publisher).Consume(),
                                      TaskCreationOptions.LongRunning);
        }

        public Task<bool> Process(EventTransaction eventTransaction)
        {
            var tcs = new TaskCompletionSource<bool>();
            var transactionTask = new TransactionTask(eventTransaction, tcs);

            _writerQueue.Add(transactionTask);

            return tcs.Task;
        }

        public IEnumerable<Event> GetEventsForAggregate(Guid aggregateId)
        {
            return _eventRepository.GetEventsForAggregate(aggregateId);
        }

        public IEnumerable<Event> GetEventsForAggregate(Guid aggregateId, int largerThan)
        {
            return _eventRepository.GetEventsForAggregate(aggregateId, largerThan);
        }

        public IEnumerable<Event> GetAllEvents(int from, int to)
        {
            return _eventRepository.GetAllEvents(from, to);
        }

        public void Dispose()
        {
            _writerQueue.CompleteAdding();
            _writerRunner.Wait();
        }
    }
}