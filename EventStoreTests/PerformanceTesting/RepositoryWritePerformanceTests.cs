using NUnit.Framework;
using SimpleEventStore;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using EventStoreTests.HelperClasses;

namespace EventStoreTests.PerformenceTesting
{
    [TestFixture(Category = "Performance")]
    public class RepositoryWritePerformanceTests
    {
        [Test]
        [Ignore("Performance test")]
        public void SaveOneMillionEventsWithSqliteAndDummyPublisher()
        {
            int numberOfEvents = 1000000;
            int serialId = 0;

            IEventStoreBuilder builder = new EventStoreBuilder();
            var eventStore = builder.UseSQLiteRepository()
                                    .Configuration(@"data source=c:\temp\sqliteevents.db;journal_mode=WAL;")
                                    .UseCustom(new DummyEventPublisher())
                                    .Build();

            var before = DateTime.Now;

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

            var after = DateTime.Now;

            var timeInMilliseconds = (after - before).TotalMilliseconds;
            var rate = numberOfEvents / (after - before).TotalSeconds;

            eventStore.Dispose();

            Assert.Pass(string.Format("Added {0} in {1} milliseconds, which is a rate of {2} per second", numberOfEvents, timeInMilliseconds, rate));
        }

        [Test]
        [Ignore("Performance test")]
        public void SaveOneMillionEventsWithLMDBAndDummyPublisher()
        {
            int numberOfEvents = 1000000;
            int serialId = 0;

            //For some reason, the values higher than 500 MB does not work.

            //TB = 1099511627776 
            //GB = 1073741824
            //500MB = 524288000
            //100MB = 104857600

            IEventStoreBuilder builder = new EventStoreBuilder();
            var eventStore = builder.UseLMDBRepository()
                                    .Configuration(@"c:\temp\lmdbevents", 2, 524288000)
                                    .UseCustom(new DummyEventPublisher())
                                    .Build();

            var before = DateTime.Now;

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

            var after = DateTime.Now;

            var timeInMilliseconds = (after - before).TotalMilliseconds;
            var rate = numberOfEvents / (after - before).TotalSeconds;

            eventStore.Dispose();

            Assert.Pass(string.Format("Added {0} in {1} milliseconds, which is a rate of {2} per second", numberOfEvents, timeInMilliseconds, rate));
        }
    }
}
