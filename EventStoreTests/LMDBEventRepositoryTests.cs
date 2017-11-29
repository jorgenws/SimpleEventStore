using SimpleEventStore;
using Xunit;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Events;
using EventSerialization;

namespace EventStoreTests
{
    
    public class LMDBEventRepositoryTests
    {
        const string EnvironmentPath = @"c:\lmdb";

        private readonly Guid _aggregateId = Guid.Parse("{54A89539-D4CA-4061-AA6A-3F4719D8EBF3}");


        public LMDBEventRepositoryTests()
        {
            //Todo
            //Remove all files and folders in C:\lmdb
        }

        [Fact]
        public void EventIsSavedAndLoadedSuccesfully()
        {
            const string somethingThatHappend = "some data that has happend";

            string tempEnvironmentPath = Path.Combine(EnvironmentPath, Guid.NewGuid().ToString("D"));

            var repository = new LMDBEventRepository(new LMDBRepositoryConfiguration(tempEnvironmentPath, new ProtobufEventsSerializer())); ;

            var events = new List<EventTransaction>
            {
                new EventTransaction
                {
                    Events = new[] {
                    new Event
                    {
                        AggregateId = _aggregateId,
                        SerialId = 0,
                        SerializedEvent = Encoding.UTF8.GetBytes(somethingThatHappend)
                    }
                }
                }
            };

            var success = repository.WriteEvents(events);

            var eventsForAggregate = repository.GetEventsForAggregate(_aggregateId);

            Assert.True(success);

            Assert.NotEmpty(eventsForAggregate);
            Assert.Equal(somethingThatHappend, Encoding.UTF8.GetString(eventsForAggregate.First().SerializedEvent));

            //Cleanup
            repository.Dispose();
            RemoveDataFromPreviousRun(tempEnvironmentPath);
        }

        [Fact]
        public void AllEventsInTransactionAreSavedAndLoadedSuccesfully()
        {
            const string somethingThatHappend = "some data that has happend";

            string tempEnvironmentPath = Path.Combine(EnvironmentPath, Guid.NewGuid().ToString("D"));

            var repository = new LMDBEventRepository(new LMDBRepositoryConfiguration(tempEnvironmentPath, new ProtobufEventsSerializer()));

            var events = new List<EventTransaction>
            {
                new EventTransaction
                {
                    Events = new[] {
                    new Event
                    {
                        AggregateId = _aggregateId,
                        SerialId = 0,
                        SerializedEvent = Encoding.UTF8.GetBytes(somethingThatHappend)
                    },
                    new Event
                    {
                        AggregateId = _aggregateId,
                        SerialId = 1,
                        SerializedEvent = Encoding.UTF8.GetBytes(somethingThatHappend)
                    }
                }
                }
            };

            var success = repository.WriteEvents(events);

            var eventsForAggregate = repository.GetEventsForAggregate(_aggregateId);

            Assert.True(success);

            Assert.Equal(2, eventsForAggregate.Length);
            Assert.Equal(somethingThatHappend, Encoding.UTF8.GetString(eventsForAggregate[0].SerializedEvent));
            Assert.Equal(somethingThatHappend, Encoding.UTF8.GetString(eventsForAggregate[1].SerializedEvent));

            //Cleanup
            repository.Dispose();
            RemoveDataFromPreviousRun(tempEnvironmentPath);
        }

        [Fact]
        public void MultipleEventsAreSavedAndLoadedSuccesfully()
        {
            const string somethingThatHappend = "some data that has happend";

            string tempEnvironmentPath = Path.Combine(EnvironmentPath, Guid.NewGuid().ToString("D"));

            var repository = new LMDBEventRepository(new LMDBRepositoryConfiguration(tempEnvironmentPath, new ProtobufEventsSerializer()));

            var events = new List<EventTransaction>
            {
                new EventTransaction
                {
                    Events = new[] {
                    new Event
                    {
                        AggregateId = _aggregateId,
                        SerialId = 0,
                        SerializedEvent = Encoding.UTF8.GetBytes(somethingThatHappend)
                    }
                }
                }
            };
            var events2 = new List<EventTransaction>
            {
                new EventTransaction
                {
                    Events = new[] {
                    new Event
                    {
                        AggregateId = _aggregateId,
                        SerialId = 1,
                        SerializedEvent = Encoding.UTF8.GetBytes(somethingThatHappend)
                    }
                }
                }
            };

            var success = repository.WriteEvents(events);
            var success2 = repository.WriteEvents(events2);

            var eventsForAggregate = repository.GetEventsForAggregate(_aggregateId);

            Assert.True(success);
            Assert.True(success2);

            Assert.Equal(2, eventsForAggregate.Length);
            Assert.Equal(somethingThatHappend, Encoding.UTF8.GetString(eventsForAggregate.First().SerializedEvent));

            //Cleanup
            repository.Dispose();
            RemoveDataFromPreviousRun(tempEnvironmentPath);
        }

        [Fact]
        public void EventsFromMultipleAggregateAreSavedReturnsAllEventsForOneAggregate()
        {
            Guid aggregateId2 = Guid.Parse("{EA623EF5-A370-4CDB-8D8C-680CE89FD799}");

            string tempEnvironmentPath = Path.Combine(EnvironmentPath, Guid.NewGuid().ToString("D"));

            var repository = new LMDBEventRepository(new LMDBRepositoryConfiguration(tempEnvironmentPath, new ProtobufEventsSerializer()));

            var events = new List<EventTransaction>
            {
                new EventTransaction
                {
                    Events = new[]
                {
                    new Event
                    {
                        AggregateId = _aggregateId,
                        SerialId = 0,
                        SerializedEvent = Encoding.UTF8.GetBytes("some data that has happend")
                    },
                    new Event
                    {
                        AggregateId = _aggregateId,
                        SerialId = 1,
                        SerializedEvent = Encoding.UTF8.GetBytes("Something else")
                    }
                }
                }
            };

            const string eventData = "some data";
            events.Add(new EventTransaction
            {
                Events = new[]
                {
                    new Event
                    {
                        AggregateId = aggregateId2,
                        SerialId = 3,
                        SerializedEvent = Encoding.UTF8.GetBytes(eventData)
                    }
                }
            });

            repository.WriteEvents(events);

            var eventsForAggregate = repository.GetEventsForAggregate(aggregateId2);

            Assert.Single(eventsForAggregate);
            Assert.Equal(eventData, Encoding.UTF8.GetString(eventsForAggregate.First().SerializedEvent));

            //Cleanup
            repository.Dispose();
            RemoveDataFromPreviousRun(tempEnvironmentPath);
        }

        [Fact]
        public void EventsFromMultipleAggregateAreSavedReturnsAllEventsForOneAggregateAfterGivenSerialNumber()
        {
            Guid aggregateId2 = Guid.Parse("{EA623EF5-A370-4CDB-8D8C-680CE89FD799}");
            const string eventData1 = "Something else";

            string tempEnvironmentPath = Path.Combine(EnvironmentPath, Guid.NewGuid().ToString("D"));

            var repository = new LMDBEventRepository(new LMDBRepositoryConfiguration(tempEnvironmentPath, new ProtobufEventsSerializer()));

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
                        SerializedEvent = Encoding.UTF8.GetBytes(eventData)
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
                        SerializedEvent = Encoding.UTF8.GetBytes("some data that has happend")
                    },
                    new Event
                    {
                        AggregateId = _aggregateId,
                        SerialId = 2,
                        SerializedEvent = Encoding.UTF8.GetBytes(eventData1)
                    }
                }
            });

            repository.WriteEvents(events);

            var eventsForAggregate = repository.GetEventsForAggregate(_aggregateId, 1);

            Assert.Single(eventsForAggregate);
            Assert.Equal(eventData1, Encoding.UTF8.GetString(eventsForAggregate.First().SerializedEvent));

            //Cleanup
            repository.Dispose();
            RemoveDataFromPreviousRun(tempEnvironmentPath);
        }

        [Fact]
        public void EventsFromMultipleAggregatesAreSavedAndAllEventsAreLoaded()
        {
            const string somethingThatHappend = "some data that happend";

            string tempEnvironmentPath = Path.Combine(EnvironmentPath, Guid.NewGuid().ToString("D"));

            var repository = new LMDBEventRepository(new LMDBRepositoryConfiguration(tempEnvironmentPath, new ProtobufEventsSerializer()));

            var events = new List<EventTransaction>
            {
                new EventTransaction
                {
                    Events = new[] {
                    new Event
                    {
                        AggregateId = Guid.NewGuid(),
                        SerialId = 0,
                        SerializedEvent = Encoding.UTF8.GetBytes(somethingThatHappend)
                    }
                }
                }
            };

            repository.WriteEvents(events);

            events.Clear();

            events.Add(new EventTransaction
            {
                Events = new[] {
                    new Event
                    {
                        AggregateId = Guid.NewGuid(),
                        SerialId = 1,
                        SerializedEvent = Encoding.UTF8.GetBytes(somethingThatHappend)
                    }
                }
            });

            repository.WriteEvents(events);

            var x = repository.GetAllEvents(0, 1);

            Assert.NotEmpty(x);

            //Cleanup
            repository.Dispose();
            RemoveDataFromPreviousRun(tempEnvironmentPath);
        }

        [Fact]
        public void NextSerialNumberReturnsNumberIncreasingWithOne()
        {
            string tempEnvironmentPath = Path.Combine(EnvironmentPath, Guid.NewGuid().ToString("D"));

            var repository = new LMDBEventRepository(new LMDBRepositoryConfiguration(tempEnvironmentPath, new ProtobufEventsSerializer()));

            var first = repository.NextSerialNumber();
            var second = repository.NextSerialNumber();
            var third = repository.NextSerialNumber();

            Assert.Equal(0, first);
            Assert.Equal(1, second);
            Assert.Equal(2, third);

            //Cleanup
            repository.Dispose();
            RemoveDataFromPreviousRun(tempEnvironmentPath);
        }

        private void RemoveDataFromPreviousRun(string tempEnvironmentPath)
        {
            var datafile = Path.Combine(tempEnvironmentPath, "data.mdb");
            if (File.Exists(datafile))
                File.Delete(datafile);

            var lockfile = Path.Combine(tempEnvironmentPath, "lock.mdb");
            if (File.Exists(lockfile))
                File.Delete(lockfile);

            if (Directory.Exists(tempEnvironmentPath))
                Directory.Delete(tempEnvironmentPath);
        }
    }
}
