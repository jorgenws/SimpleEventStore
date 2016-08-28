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
            int numberOfEvents = 100000;

            IEventStoreBuilder builder = new EventStoreBuilder();
            var eventStore = builder.UseSQLiteRepository()
                                    .Configuration(@"data source=d:\events.db;Version=3;Mode=WAL")
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
