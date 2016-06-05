using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace SimpleEventStore
{
    public class EventStore : IDisposable
    {
        private readonly BlockingCollection<EventTransaction> _writerQueue;
        private readonly IEventRepository _eventRepository;
        private Task _writerRunner;

        private const int BufferSize = 100000;

        public EventStore(IEventRepository repository)
        {
            _eventRepository = repository;
            _writerQueue = new BlockingCollection<EventTransaction>(BufferSize);
            
            //ToDo: Look into using continuation to catch that the task died and recreate it if possible.
            _writerRunner = Task.Factory.StartNew(() => new EventConsumer(_writerQueue, repository).Consume(),
                                                  TaskCreationOptions.LongRunning);
        }

        public async Task Process(EventTransaction eventTransaction)
        {
            _writerQueue.Add(eventTransaction);
            await eventTransaction.WaitAsync();
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
