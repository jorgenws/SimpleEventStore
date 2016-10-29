﻿using SimpleEventStore;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Events;
using EventSerialization;

namespace EventStoreTests
{
    [TestFixture(Category = "Integration")]
    public class LMDBEventRepositoryTests
    {
        const string EnvironmentPath = @"c:\lmdb";

        private readonly Guid _aggregateId = Guid.Parse("{54A89539-D4CA-4061-AA6A-3F4719D8EBF3}");

        private LMDBEventRepository _lmdbEventRepository;

        [OneTimeSetUp]
        public void TestFixtureSetUp()
        {
            RemoveDataFromPreviousRun();
        }

        [SetUp]
        public void SetUp()
        {
            _lmdbEventRepository = new LMDBEventRepository(new LMDBRepositoryConfiguration(EnvironmentPath, new ProtobufEventsSerializer()));
        }

        [Test]
        public void EventIsSavedAndLoadedSuccesfully()
        {
            const string somethingThatHappend = "some data that has happend";

            var repository = _lmdbEventRepository;

            var events = new List<EventTransaction>();
            events.Add(new EventTransaction
            {
                Events = new[] {
                    new Event
                    {
                        AggregateId = _aggregateId,
                        SerialId = 0,
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
                Events = new[] {
                    new Event
                    {
                        AggregateId = _aggregateId,
                        SerialId = 0,
                        SerializedEvent = Encoding.UTF8.GetBytes(somethingThatHappend)
                    }
                }
            });
            var events2 = new List<EventTransaction>();
            events2.Add(new EventTransaction
            {
                Events = new[] {
                    new Event
                    {
                        AggregateId = _aggregateId,
                        SerialId = 1,
                        SerializedEvent = Encoding.UTF8.GetBytes(somethingThatHappend)
                    }
                }
            });

            var success = repository.WriteEvents(events);
            var success2 = repository.WriteEvents(events2);

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
            });

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

            Assert.AreEqual(1, eventsForAggregate.Length);
            Assert.AreEqual(eventData1, Encoding.UTF8.GetString(eventsForAggregate.First().SerializedEvent));
        }

        [Test]
        public void EventsFromMultipleAggregatesAreSavedAndAllEventsAreLoaded()
        {
            const string somethingThatHappend = "some data that happend";

            var repository = _lmdbEventRepository;

            var events = new List<EventTransaction>();
            events.Add(new EventTransaction
            {
                Events = new[] {
                    new Event
                    {
                        AggregateId = Guid.NewGuid(),
                        SerialId = 0,
                        SerializedEvent = Encoding.UTF8.GetBytes(somethingThatHappend)
                    }
                }
            });

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

            CollectionAssert.IsNotEmpty(x);
        }

        [Test]
        public void NextSerialNumberReturnsNumberIncreasingWithOne()
        {
            var repository = _lmdbEventRepository;

            var first = repository.NextSerialNumber();
            var second = repository.NextSerialNumber();
            var third = repository.NextSerialNumber();

            Assert.AreEqual(0, first);
            Assert.AreEqual(1, second);
            Assert.AreEqual(2, third);
        }

        [TearDown]
        public void TearDown()
        {
            _lmdbEventRepository.Dispose();
            RemoveDataFromPreviousRun();
        }

        private void RemoveDataFromPreviousRun()
        {
            var datafile = Path.Combine(EnvironmentPath, "data.mdb");
            if (File.Exists(datafile))
                File.Delete(datafile);

            var lockfile = Path.Combine(EnvironmentPath, "lock.mdb");
            if (File.Exists(lockfile))
                File.Delete(lockfile);
        }
    }
}
