using Events;
using EventSerialization;
using LightningDB;
using System;
using System.Collections.Generic;
using System.Linq;

using System.Runtime.CompilerServices;
[assembly: InternalsVisibleTo("EventStoreTests")]

namespace SimpleEventStore
{
    internal class LMDBEventRepository : IEventRepository
    {
        private const string EventDb = "eventDb";
        private const string AggregateIndex = "aggregateIndex";

        private readonly LightningEnvironment _environment;
        private readonly IBinaryEventsSerializer _serializer;

        private int _nextSerialNumber;

        public LMDBEventRepository(LMDBRepositoryConfiguration configuration)
        {
            _serializer = configuration.Serializer;
            _environment = new LightningEnvironment(configuration.EnvironmentPath)
            {
                MaxDatabases = configuration.MaxDatabases,
                MapSize = configuration.MapSize
            };
            _environment.Open();

            _nextSerialNumber = InitSerialNumber();
        }

        public bool WriteEvents(List<EventTransaction> eventTransactions)
        {
            try
            {
                using (var tx = _environment.BeginTransaction())
                using (var eventDb = tx.OpenDatabase(EventDb, new DatabaseConfiguration { Flags = DatabaseOpenFlags.Create | DatabaseOpenFlags.IntegerKey }))
                using (var aggregateIndex = tx.OpenDatabase(AggregateIndex, new DatabaseConfiguration { Flags = DatabaseOpenFlags.Create | DatabaseOpenFlags.DuplicatesSort }))
                {
                    foreach (var transaction in eventTransactions)
                    {
                        foreach (var @event in transaction.Events)
                        {
                            byte[] nextSerialNumber = BitConverter.GetBytes(@event.SerialId);
                            byte[] aggregateId = @event.AggregateId.ToByteArray();

                            var serializedEvent = _serializer.Serialize(@event);
                            tx.Put(eventDb, nextSerialNumber, serializedEvent, PutOptions.AppendData);
                            tx.Put(aggregateIndex, aggregateId, nextSerialNumber, PutOptions.AppendDuplicateData);
                        }
                    }

                    tx.Commit();
                }
            }catch(Exception)
            {
                return false;
            }

            return true;
        }

        private int InitSerialNumber()
        {
            byte[] key;

            using (var tx = _environment.BeginTransaction(TransactionBeginFlags.ReadOnly))
            {
                LightningDatabase db;
                try
                {
                    db = tx.OpenDatabase(EventDb);
                }
                catch (LightningException)
                {
                    return 0;
                }

                var c = tx.CreateCursor(db);
                c.MoveToLast();
                key = c.GetCurrent().Key;
            }

            return BitConverter.ToInt32(key, 0);
        }

        public Event[] GetEventsForAggregate(Guid aggregateId)
        {
            var events = new List<Event>();
            using (var tx = _environment.BeginTransaction(TransactionBeginFlags.ReadOnly))
            using (var eventsDb = tx.OpenDatabase(EventDb))
            using (var aggregateIndex = tx.OpenDatabase(AggregateIndex))
            {
                var aggregateIndexCursor = tx.CreateCursor(aggregateIndex);

                aggregateIndexCursor.MoveTo(aggregateId.ToByteArray());
                aggregateIndexCursor.MoveToFirstDuplicate();

                do
                {
                    KeyValuePair<byte[], byte[]> eventId = aggregateIndexCursor.GetCurrent();
                    var serializedEvent = tx.Get(eventsDb, eventId.Value);
                    var @event = _serializer.DeserializeEvent(serializedEvent);
                    events.Add(@event);
                } while (aggregateIndexCursor.MoveNextDuplicate());
            }

            return events.ToArray();
        }

        public Event[] GetEventsForAggregate(Guid aggregateId, int largerThan)
        {
            byte[] largerThanAsBytes = BitConverter.GetBytes(largerThan);

            var events = new List<Event>();
            using (var tx = _environment.BeginTransaction(TransactionBeginFlags.ReadOnly))
            using (var eventsDb = tx.OpenDatabase(EventDb))
            using (var aggregateIndex = tx.OpenDatabase(AggregateIndex))
            {
                var aggregateIndexCursor = tx.CreateCursor(aggregateIndex);

                aggregateIndexCursor.MoveToFirstValueAfter(aggregateId.ToByteArray(), largerThanAsBytes);

                //MoveToFirstValueAfter stops at the first matching or greater.
                //Need to check if we are at a match or at a item that is greater the the serial number
                if (aggregateIndexCursor.GetCurrent().Value.SequenceEqual(largerThanAsBytes))
                    aggregateIndexCursor.MoveNextDuplicate();
              
                do
                {
                    KeyValuePair<byte[], byte[]> eventId = aggregateIndexCursor.GetCurrent();
                    var serializedEvent = tx.Get(eventsDb, eventId.Value);
                    var @event = _serializer.DeserializeEvent(serializedEvent);
                    events.Add(@event);
                } while (aggregateIndexCursor.MoveNextDuplicate());
            }

            return events.ToArray();
        }

        public Event[] GetAllEvents(int @from, int to)
        {
            byte[] fromAsBytes = BitConverter.GetBytes(@from);
            byte[] toAsBytes = BitConverter.GetBytes(@to);

            var events = new List<Event>();
            using (var tx = _environment.BeginTransaction(TransactionBeginFlags.ReadOnly))
            using (var eventsDb = tx.OpenDatabase(EventDb))
            {
                var eventsCursor = tx.CreateCursor(eventsDb);
                //eventsCursor.MoveToAndGet(fromAsBytes); //Buggy
                eventsCursor.MoveTo(fromAsBytes);
                eventsCursor.GetCurrent();
                var @event = _serializer.DeserializeEvent(eventsCursor.Current.Value);
                events.Add(@event);

                bool reachedTo = false;
                while (!reachedTo && eventsCursor.MoveNext())
                {
                    var current = eventsCursor.GetCurrent();
                    if (current.Key.SequenceEqual(toAsBytes))
                        reachedTo = true;

                    events.Add(new Event {SerializedEvent = current.Value});
                }
            }

            return events.ToArray();
        }

        public void Dispose()
        {
            _environment.Dispose();
        }

        public int NextSerialNumber()
        {
            return _nextSerialNumber++;
        }

        public void ResetSerialNumber()
        {
            _nextSerialNumber = InitSerialNumber();
        }
    }
}