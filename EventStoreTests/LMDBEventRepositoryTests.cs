using SimpleEventStore;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace EventStoreTests
{
    [TestFixture]
    public class LMDBEventRepositoryTests
    {
        const string EnvironmentPath = @"c:\lmdb";

        private readonly Guid _aggregateId = Guid.Parse("{54A89539-D4CA-4061-AA6A-3F4719D8EBF3}");

        private LMDBEventRepository _lmdbEventRepository;

        [SetUp]
        public void SetUp()
        {
            _lmdbEventRepository = new LMDBEventRepository(new LMDBRepositoryConfiguration(EnvironmentPath));
        }

        [Test]
        public void EventIsSavedAndLoadedSuccesfully()
        {
            const string somethingThatHappend = "some data that has happend";

            var repository = _lmdbEventRepository;

            var events = new List<EventTransaction>();
            events.Add(new EventTransaction
            {
                AggregateId = _aggregateId,
                Events = new[] {
                    new Event
                    {
                        SerializedEvent = Encoding.UTF8.GetBytes(somethingThatHappend)
                    }
                }
            });

            var success = repository.WriteEvents(events);

            var eventsForAggregate = repository.GetEventsForAggregate(_aggregateId);

            Assert.IsTrue(success);

            CollectionAssert.IsNotEmpty(eventsForAggregate);
            Assert.AreEqual(somethingThatHappend, Encoding.UTF8.GetString(eventsForAggregate.First().SerializedEvent));

        }

        [Test]
        public void AllEventsInTransactionAreSavedAndLoadedSuccesfully()
        {
            const string somethingThatHappend = "some data that has happend";

            var repository = _lmdbEventRepository;

            var events = new List<EventTransaction>();
            events.Add(new EventTransaction
            {
                AggregateId = _aggregateId,
                Events = new[] {
                    new Event
                    {
                        SerializedEvent = Encoding.UTF8.GetBytes(somethingThatHappend)
                    },
                    new Event
                    {
                        SerializedEvent = Encoding.UTF8.GetBytes(somethingThatHappend)
                    }
                }
            });

            var success = repository.WriteEvents(events);

            var eventsForAggregate = repository.GetEventsForAggregate(_aggregateId);

            Assert.IsTrue(success);

            Assert.AreEqual(2, eventsForAggregate.Length);
            Assert.AreEqual(somethingThatHappend, Encoding.UTF8.GetString(eventsForAggregate[0].SerializedEvent));
            Assert.AreEqual(somethingThatHappend, Encoding.UTF8.GetString(eventsForAggregate[1].SerializedEvent));
        }

        [Test]
        public void MultipleEventsAreSavedAndLoadedSuccesfully()
        {
            const string somethingThatHappend = "some data that has happend";

            var repository = _lmdbEventRepository;

            var events = new List<EventTransaction>();
            events.Add(new EventTransaction
            {
                AggregateId = _aggregateId,
                Events = new[] {
                    new Event
                    {
                        SerializedEvent = Encoding.UTF8.GetBytes(somethingThatHappend)
                    }
                }
            });

            var success = repository.WriteEvents(events);
            var success2 = repository.WriteEvents(events);

            var eventsForAggregate = repository.GetEventsForAggregate(_aggregateId);

            Assert.IsTrue(success);
            Assert.IsTrue(success2);

            Assert.AreEqual(2, eventsForAggregate.Length);
            Assert.AreEqual(somethingThatHappend, Encoding.UTF8.GetString(eventsForAggregate.First().SerializedEvent));
        }

        [Test]
        public void EventsFromMultipleAggregateAreSavedReturnsAllEventsForOneAggregate()
        {
            Guid aggregateId2 = Guid.Parse("{EA623EF5-A370-4CDB-8D8C-680CE89FD799}");

            var repository = _lmdbEventRepository;

            var events = new List<EventTransaction>();
            events.Add(new EventTransaction
            {
                AggregateId = _aggregateId,
                Events = new[]
                {
                    new Event
                    {
                        SerializedEvent = Encoding.UTF8.GetBytes("some data that has happend")
                    },
                    new Event
                    {
                        SerializedEvent = Encoding.UTF8.GetBytes("Something else")
                    }
                }
            });

            const string eventData = "some data";
            events.Add(new EventTransaction
            {
                AggregateId = aggregateId2,
                Events = new[]
                {
                    new Event
                    {
                        SerializedEvent = Encoding.UTF8.GetBytes(eventData)
                    }
                }
            });

            repository.WriteEvents(events);

            var eventsForAggregate = repository.GetEventsForAggregate(aggregateId2);

            Assert.AreEqual(1, eventsForAggregate.Length);
            Assert.AreEqual(eventData, Encoding.UTF8.GetString(eventsForAggregate.First().SerializedEvent));
        }

        [Test]
        public void EventsFromMultipleAggregateAreSavedReturnsAllEventsForOneAggregateAfterGivenSerialNumber()
        {
            Guid aggregateId2 = Guid.Parse("{EA623EF5-A370-4CDB-8D8C-680CE89FD799}");
            const string eventData1 = "Something else";

            var repository = _lmdbEventRepository;

            var events = new List<EventTransaction>();
            const string eventData = "some data";
            events.Add(new EventTransaction
            {
                AggregateId = aggregateId2,
                Events = new[]
                {
                    new Event
                    {
                        SerializedEvent = Encoding.UTF8.GetBytes(eventData)
                    }
                }
            });
            events.Add(new EventTransaction
            {
                AggregateId = _aggregateId,
                Events = new[]
                {
                    new Event
                    {
                        SerializedEvent = Encoding.UTF8.GetBytes("some data that has happend")
                    },
                    new Event
                    {
                        SerializedEvent = Encoding.UTF8.GetBytes(eventData1)
                    }
                }
            });

            repository.WriteEvents(events);

            var eventsForAggregate = repository.GetEventsForAggregate(_aggregateId, 1);

            Assert.AreEqual(1, eventsForAggregate.Length);
            Assert.AreEqual(eventData1, Encoding.UTF8.GetString(eventsForAggregate.First().SerializedEvent));
        }



        [TearDown]
        public void TearDown()
        {
            _lmdbEventRepository.Dispose();

            var datafile = Path.Combine(EnvironmentPath, "data.mdb");
            if (File.Exists(datafile))
                File.Delete(datafile);

            var lockfile = Path.Combine(EnvironmentPath, "lock.mdb");
            if (File.Exists(lockfile))
                File.Delete(lockfile);
        }
    }
}
