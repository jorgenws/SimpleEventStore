using NUnit.Framework;
using SimpleEventStore;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using EventStoreTests.HelperClasses;

namespace EventStoreTests.PerformanceTesting
{
    [TestFixture(Category = "Performance")]
    public class RepositoryReadPerformanceTests
    {
        [Test]
        [Ignore("Performance test")]
        public void ReadOneMillionFromSqlite()
        {
            int numberOfEvents = 1000000;
            int serialId = 0;

            IEventStoreBuilder builder = new EventStoreBuilder();
            var eventStore = builder.UseSQLiteRepository()
                                    .Configuration(@"data source=c:\temp\sqliteevents.db;journal_mode=WAL;")
                                    .UseCustom(new DummyEventPublisher())
                                    .Build();

            var tasks = new ConcurrentBag<Task>();
            foreach (int i in Enumerable.Range(0, numberOfEvents))
            {
                tasks.Add(eventStore.Process(new EventTransaction
                {
                    Events = new[] {
                    new Event
                    {
                        AggregateId = Guid.NewGuid(),
                        SerialId = serialId++,
                        SerializedEvent = BitConverter.GetBytes(i),
                        EventType = "A type of event"
                    }
                }
                }));
            }

            Task.WhenAll(tasks.ToArray()).Wait();

            var before = DateTime.Now;

            var x = eventStore.GetAllEvents(0, 1000000);

            var after = DateTime.Now;

            var timeInMilliseconds = (after - before).TotalMilliseconds;
            var rate = numberOfEvents / (after - before).TotalSeconds;

            eventStore.Dispose();

            Assert.Pass(string.Format("Read {0} in {1} milliseconds, which is a rate of {2} per second", numberOfEvents, timeInMilliseconds, rate));
        }

        [Test]
        [Ignore("Performance test")]
        public void ReadOneMillionFromLMDB()
        {
            int numberOfEvents = 1000000;
            int serialId = 0;

            IEventStoreBuilder builder = new EventStoreBuilder();
            var eventStore = builder.UseLMDBRepository()
                                    .Configuration(@"c:\temp\lmdbevents", 2, 524288000)
                                    .UseCustom(new DummyEventPublisher())
                                    .Build();

            var tasks = new ConcurrentBag<Task>();
            foreach (int i in Enumerable.Range(0, numberOfEvents))
            {
                tasks.Add(eventStore.Process(new EventTransaction
                {
                    Events = new[] {
                    new Event
                    {
                        AggregateId = Guid.NewGuid(),
                        SerialId = serialId++,
                        SerializedEvent = BitConverter.GetBytes(i),
                        EventType = "A type of event"
                    }
                }
                }));
            }

            Task.WhenAll(tasks.ToArray()).Wait();

            var before = DateTime.Now;

            var x = eventStore.GetAllEvents(0, 1000000);

            var after = DateTime.Now;

            var timeInMilliseconds = (after - before).TotalMilliseconds;
            var rate = numberOfEvents / (after - before).TotalSeconds;

            eventStore.Dispose();

            Assert.Pass(string.Format("Read {0} in {1} milliseconds, which is a rate of {2} per second", numberOfEvents, timeInMilliseconds, rate));
        }
    }
}
