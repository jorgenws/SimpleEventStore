using EventStore;
using NUnit.Framework;
using System;
using System.Collections.Generic;

namespace EventStoreTests
{
    [TestFixture]
    public class SQLiteEventRepositoryTests
    {
        [Test]
        public void test()
        {
            var repository = new SQLiteEventRepository();

            var events = new List<EventTransaction>();
            events.Add(new EventTransaction
            {
                UserId = Guid.NewGuid(),
                CompanyId = Guid.NewGuid(),
                Events = new[] {
                    new Event
                    {
                        Id = Guid.NewGuid(),
                        SerializedEvent = "some data that has happend"
                    }
                }
            });

            var success = repository.WriteEvents(events);

            Assert.IsTrue(success);
        }

        [Test]
        public void test2()
        {
            var repository = new SQLiteEventRepository();

            var events = new List<EventTransaction>();
            events.Add(new EventTransaction
            {
                UserId = Guid.NewGuid(),
                CompanyId = Guid.NewGuid(),
                Events = new[] {
                    new Event
                    {
                        Id = Guid.NewGuid(),
                        SerializedEvent = "some data that has happend"
                    }
                }
            });

            var success = repository.WriteEvents(events);

            var max = repository.NextSerialNumber();

            Assert.AreEqual(1, max);
        }
    }
}
