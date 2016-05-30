﻿using EventStore;
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

        public Guid aggregateId = Guid.Parse("{54A89539-D4CA-4061-AA6A-3F4719D8EBF3}");

        private LMDBEventRepository _lmdbEventRepository;

        [SetUp]
        public void SetUp()
        {
            _lmdbEventRepository = new LMDBEventRepository(new LMDBRepositoryConfiguration(EnvironmentPath));
        }

        [Test]
        public void EventIsSavedAndLoadedSuccesfully()
        {
            const string SomethingThatHappend = "some data that has happend";

            var repository = _lmdbEventRepository;

            var events = new List<EventTransaction>();
            events.Add(new EventTransaction
            {
                AggregateId = aggregateId,
                Events = new[] {
                    new Event
                    {
                        SerializedEvent = Encoding.UTF8.GetBytes(SomethingThatHappend)
                    }
                }
            });

            var success = repository.WriteEvents(events);

            var eventsForAggregate = repository.GetEventsForAggregate(aggregateId);

            Assert.IsTrue(success);

            CollectionAssert.IsNotEmpty(eventsForAggregate);
            Assert.AreEqual(SomethingThatHappend, Encoding.UTF8.GetString(eventsForAggregate.First().SerializedEvent));

        }

        [Test]
        public void AllEventsInTransactionAreSavedAndLoadedSuccesfully()
        {
            const string SomethingThatHappend = "some data that has happend";

            var repository = _lmdbEventRepository;

            var events = new List<EventTransaction>();
            events.Add(new EventTransaction
            {
                AggregateId = aggregateId,
                Events = new[] {
                    new Event
                    {
                        SerializedEvent = Encoding.UTF8.GetBytes(SomethingThatHappend)
                    },
                    new Event
                    {
                        SerializedEvent = Encoding.UTF8.GetBytes(SomethingThatHappend)
                    }
                }
            });

            var success = repository.WriteEvents(events);

            var eventsForAggregate = repository.GetEventsForAggregate(aggregateId);

            Assert.IsTrue(success);

            Assert.AreEqual(2, eventsForAggregate.Length);
            Assert.AreEqual(SomethingThatHappend, Encoding.UTF8.GetString(eventsForAggregate[0].SerializedEvent));
            Assert.AreEqual(SomethingThatHappend, Encoding.UTF8.GetString(eventsForAggregate[1].SerializedEvent));
        }

        [Test]
        public void MultipleEventsAreSavedAndLoadedSuccesfully()
        {
            const string SomethingThatHappend = "some data that has happend";

            var repository = _lmdbEventRepository;

            var events = new List<EventTransaction>();
            events.Add(new EventTransaction
            {
                AggregateId = aggregateId,
                Events = new[] {
                    new Event
                    {
                        SerializedEvent = Encoding.UTF8.GetBytes(SomethingThatHappend)
                    }
                }
            });

            var success = repository.WriteEvents(events);
            var success2 = repository.WriteEvents(events);

            var eventsForAggregate = repository.GetEventsForAggregate(aggregateId);

            Assert.IsTrue(success);
            Assert.IsTrue(success2);

            Assert.AreEqual(2, eventsForAggregate.Length);
            Assert.AreEqual(SomethingThatHappend, Encoding.UTF8.GetString(eventsForAggregate.First().SerializedEvent));
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