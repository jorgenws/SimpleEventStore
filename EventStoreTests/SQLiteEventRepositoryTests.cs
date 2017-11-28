using SimpleEventStore;
using Xunit;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Events;

namespace EventStoreTests
{
    public class SQLiteEventRepositoryTests
    {
        private readonly Guid _aggregateId = Guid.Parse("{54A89539-D4CA-4061-AA6A-3F4719D8EBF3}");
        private const string EventType = "AEventType";

        [Fact]
        public void EventIsSavedAndLoadedSuccesfully()
        {
            const string somethingThatHappend = "some data that has happend";

            var repository = GetInMemorySQLiteEventRepository();

            var events = new List<EventTransaction>();
            events.Add(new EventTransaction
            {                
                Events = new[] {
                    new Event
                    {
                        AggregateId = _aggregateId,
                        SerialId = 0,
                        SerializedEvent = Encoding.UTF8.GetBytes(somethingThatHappend),
                        EventType = EventType
                    }
                }
            });

            var success = repository.WriteEvents(events);

            var eventsForAggregate = repository.GetEventsForAggregate(_aggregateId);

            Assert.True(success);

            Assert.NotEmpty(eventsForAggregate);
            Assert.Equal(somethingThatHappend, Encoding.UTF8.GetString(eventsForAggregate.First().SerializedEvent));

        }

        [Fact]
        public void AllEventsInTransactionAreSavedAndLoadedSuccesfully()
        {
            const string somethingThatHappend = "some data that has happend";

            var repository = GetInMemorySQLiteEventRepository();

            var events = new List<EventTransaction>();
            events.Add(new EventTransaction
            {
                Events = new[] {
                    new Event
                    {
                        AggregateId = _aggregateId,
                        SerialId = 0,
                        SerializedEvent = Encoding.UTF8.GetBytes(somethingThatHappend),
                        EventType = EventType
                    },
                    new Event
                    {
                        AggregateId = _aggregateId,
                        SerialId = 1,
                        SerializedEvent = Encoding.UTF8.GetBytes(somethingThatHappend),
                        EventType = EventType
                    }
                }
            });

            var success = repository.WriteEvents(events);

            var eventsForAggregate = repository.GetEventsForAggregate(_aggregateId);

            Assert.True(success);

            Assert.Equal(2, eventsForAggregate.Length);
            Assert.Equal(somethingThatHappend, Encoding.UTF8.GetString(eventsForAggregate[0].SerializedEvent));
            Assert.Equal(somethingThatHappend, Encoding.UTF8.GetString(eventsForAggregate[1].SerializedEvent));
        }

        [Fact]
        public void MultipleEventsAreSavedAndLoadedSuccesfully()
        {
            const string somethingThatHappend = "some data that has happend";

            var repository = GetInMemorySQLiteEventRepository();

            var events = new List<EventTransaction>();
            events.Add(new EventTransaction
            {
                
                Events = new[] {
                    new Event
                    {
                        AggregateId = _aggregateId,
                        SerialId = 0,
                        SerializedEvent = Encoding.UTF8.GetBytes(somethingThatHappend),
                        EventType = EventType
                    }
                }
            });

            var success = repository.WriteEvents(events);
            var success2 = repository.WriteEvents(events);

            var eventsForAggregate = repository.GetEventsForAggregate(_aggregateId);

            Assert.True(success);
            Assert.True(success2);

            Assert.Equal(2, eventsForAggregate.Length);
            Assert.Equal(somethingThatHappend, Encoding.UTF8.GetString(eventsForAggregate.First().SerializedEvent));
        }

        [Fact]
        public void EventsFromMultipleAggregateAreSavedReturnsAllEventsForOneAggregate()
        {
            Guid aggregateId2 = Guid.Parse("{EA623EF5-A370-4CDB-8D8C-680CE89FD799}");

            var repository = GetInMemorySQLiteEventRepository();

            var events = new List<EventTransaction>();
            events.Add(new EventTransaction
            {
                Events = new[]
                {
                    new Event
                    {
                        AggregateId = _aggregateId,
                        SerialId = 0,
                        SerializedEvent = Encoding.UTF8.GetBytes("some data that has happend"),
                        EventType = EventType
                    },
                    new Event
                    {
                        AggregateId = _aggregateId,
                        SerialId = 1,
                        SerializedEvent = Encoding.UTF8.GetBytes("Something else"),
                        EventType = EventType
                    }
                }
            });

            const string eventData = "some data"; 
            events.Add(new EventTransaction
            {
                Events = new[]
                {
                    new Event
                    {
                        AggregateId = aggregateId2,
                        SerialId = 2,
                        SerializedEvent = Encoding.UTF8.GetBytes(eventData),
                        EventType = EventType
                    }
                }
            });

            repository.WriteEvents(events);

            var eventsForAggregate = repository.GetEventsForAggregate(aggregateId2);
            
            Assert.Single(eventsForAggregate);
            Assert.Equal(eventData, Encoding.UTF8.GetString(eventsForAggregate.First().SerializedEvent));
        }

        [Fact]
        public void EventsFromMultipleAggregateAreSavedReturnsAllEventsForOneAggregateAfterGivenSerialNumber()
        {
            Guid aggregateId2 = Guid.Parse("{EA623EF5-A370-4CDB-8D8C-680CE89FD799}");
            const string eventData1 = "Something else";

            var repository = GetInMemorySQLiteEventRepository();

            var events = new List<EventTransaction>();
            const string eventData = "some data";
            events.Add(new EventTransaction
            {
                Events = new[]
                {
                    new Event
                    {
                        AggregateId = aggregateId2,
                        SerialId = 0,
                        SerializedEvent = Encoding.UTF8.GetBytes(eventData),
                        EventType = EventType
                    }
                }
            });
            events.Add(new EventTransaction
            {
                Events = new[]
                {
                    new Event
                    {
                        AggregateId = _aggregateId,
                        SerialId = 1,
                        SerializedEvent = Encoding.UTF8.GetBytes("some data that has happend"),
                        EventType = EventType
                    },
                    new Event
                    {
                        AggregateId = _aggregateId,
                        SerialId = 2,
                        SerializedEvent = Encoding.UTF8.GetBytes(eventData1),
                        EventType = EventType
                    }
                }
            });

            repository.WriteEvents(events);

            var eventsForAggregate = repository.GetEventsForAggregate(_aggregateId, 1);

            Assert.Single(eventsForAggregate);
            Assert.Equal(eventData1, Encoding.UTF8.GetString(eventsForAggregate.First().SerializedEvent));
        }

        [Fact]
        public void NextSerialNumberReturnsNumberIncreasingWithOne()
        {
            var repository = GetInMemorySQLiteEventRepository();

            var first = repository.NextSerialNumber();
            var second = repository.NextSerialNumber();
            var third = repository.NextSerialNumber();

            Assert.Equal(0, first);
            Assert.Equal(1, second);
            Assert.Equal(2, third);
        }
        
        private SQLiteEventRepository GetInMemorySQLiteEventRepository()
        {
            return new SQLiteEventRepository(new SQLiteRepositoryConfiguration("Data Source=:memory:"));
        }
    }
}
