using NUnit.Framework;
using SimpleEventStore;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;

namespace EventStoreTests.PerformenceTesting
{
    [TestFixture]
    public class PerformanceTests
    {
        [Test]
        [Ignore("Performance test")]
        public void SaveHundredThousendEventsWithSqliteAndDummyPublisher()
        {
            int numberOfEvents = 1000000;

            IEventStoreBuilder builder = new EventStoreBuilder();
            var eventStore = builder.UseSQLiteRepository()
                                    .Configuration(@"data source=c:\temp\events.db;journal_mode=WAL;")
                                    .UseDummyPublisher()
                                    .Build();

            var before = DateTime.Now;

            var tasks = new ConcurrentBag<Task>();
            foreach (int i in Enumerable.Range(0, numberOfEvents))
            {
                tasks.Add(eventStore.Process(new EventTransaction
                {
                    AggregateId = Guid.NewGuid(),
                    Events = new[] {
                    new Event
                    {
                        SerializedEvent = BitConverter.GetBytes(i)
                    }
                }
                }));
            }

            Task.WhenAll(tasks.ToArray()).Wait();

            var after = DateTime.Now;

            var timeInMilliseconds = (after - before).TotalMilliseconds;
            var rate = numberOfEvents / (after - before).TotalSeconds;

            eventStore.Dispose();

            Assert.Pass(string.Format("Added {0} in {1} millisecons, which is a rate of {2} per second", numberOfEvents, timeInMilliseconds, rate));
        }

        [Test]
        //[Ignore("Performance test")]
        public void SaveHundredThousendEventsWithLMDBAndDummyPublisher()
        {
            int numberOfEvents = 1000000;

            //For some reason, the values higher than 500 MB does not work.

            //TB = 1099511627776 
            //GB = 1073741824
            //500MB = 524288000
            //100MB = 104857600

            IEventStoreBuilder builder = new EventStoreBuilder();
            var eventStore = builder.UseLMDBRepository()
                                    .Configuration(@"c:\temp\events.db", 2, 524288000)
                                    .UseDummyPublisher()
                                    .Build();

            var before = DateTime.Now;

            var tasks = new ConcurrentBag<Task>();
            foreach (int i in Enumerable.Range(0, numberOfEvents))
            {
                tasks.Add(eventStore.Process(new EventTransaction
                {
                    AggregateId = Guid.NewGuid(),
                    Events = new[] {
                    new Event
                    {
                        SerializedEvent = BitConverter.GetBytes(i)
                    }
                }
                }));
            }

            Task.WhenAll(tasks.ToArray()).Wait();

            var after = DateTime.Now;

            var timeInMilliseconds = (after - before).TotalMilliseconds;
            var rate = numberOfEvents / (after - before).TotalSeconds;

            eventStore.Dispose();

            Assert.Pass(string.Format("Added {0} in {1} millisecons, which is a rate of {2} per second", numberOfEvents, timeInMilliseconds, rate));
        }
    }
}
