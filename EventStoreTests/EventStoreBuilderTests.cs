using System;
using SimpleEventStore;
using NUnit.Framework;
using System.IO;
using EventStoreTests.HelperClasses;
using EventSerialization;

namespace EventStoreTests
{
    [TestFixture]
    public class EventStoreBuilderTests
    {
        [Test]
        public void BuildSqliteEventStore()
        {
            IEventStoreBuilder builder = new EventStoreBuilder();

            EventStore es = null;
            Assert.DoesNotThrow(() => es = builder.UseSQLiteRepository()
                                                  .Configuration("Data Source =:memory:")
                                                  .UseCustom(new DummyEventPublisher())
                                                  .Build());

            Assert.NotNull(es);
            
            //Clean up database
            es.Dispose();
        }

        [Test]
        [Category("Integration")]
        public void BuildLMDBEventStore()
        {
            IEventStoreBuilder builder = new EventStoreBuilder();

            EventStore es = null;
            Assert.DoesNotThrow(() => es = builder.UseLMDBRepository()
                                                  .Configuration(@"c:\lmdb", 2, 10485760, new ProtobufEventsSerializer())
                                                  .UseCustom(new DummyEventPublisher())
                                                  .Build());

            Assert.NotNull(es);

            //Clean up database
            es.Dispose();
        }

        [Test]
        public void BuildingWithMissingDataThrowsException()
        {
            EventStoreBuilder builder = new EventStoreBuilder();
            Assert.Throws<Exception>(() => builder.Build());
        }

        [TearDown]
        public void TearDown()
        {
            //Added to clean up after BuildLMDBEventStore

            GC.Collect();
            GC.WaitForPendingFinalizers();

            var datafile = Path.Combine(@"c:\lmdb", "data.mdb");
            if (File.Exists(datafile))
                File.Delete(datafile);

            var lockfile = Path.Combine(@"c:\lmdb", "lock.mdb");
            if (File.Exists(lockfile))
                File.Delete(lockfile);
        }
    }
}
