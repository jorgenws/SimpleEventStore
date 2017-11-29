using System;
using SimpleEventStore;
using Xunit;
using System.IO;
using EventStoreTests.HelperClasses;
using EventSerialization;

namespace EventStoreTests
{
    public class EventStoreBuilderTests
    {
        //[Fact]
        //public void BuildSqliteEventStoreDoesNotThrow()
        //{
        //    IEventStoreBuilder builder = new EventStoreBuilder();

        //    EventStore es = null;
        //    es = builder.UseSQLiteRepository()
        //                .Configuration("Data Source =:memory:")
        //                .UseCustom(new DummyEventPublisher())
        //                .Build();

        //    Assert.NotNull(es);
            
        //    //Clean up database
        //    es.Dispose();
        //}

        [Fact]
        [Trait("Category", "Integration")]
        public void BuildLMDBEventStoreDoesNotThrow()
        {
            IEventStoreBuilder builder = new EventStoreBuilder();

            EventStore es = null;
            es = builder.UseLMDBRepository()
                        .Configuration(@"c:\lmdb", 2, 10485760, new ProtobufEventsSerializer())
                        .UseCustom(new DummyEventPublisher())
                        .Build();

            Assert.NotNull(es);

            //Clean up database
            es.Dispose();


            //Cleaning up disk

            GC.Collect();
            GC.WaitForPendingFinalizers();

            var datafile = Path.Combine(@"c:\lmdb", "data.mdb");
            if (File.Exists(datafile))
                File.Delete(datafile);

            var lockfile = Path.Combine(@"c:\lmdb", "lock.mdb");
            if (File.Exists(lockfile))
                File.Delete(lockfile);
        }

        [Fact]
        public void BuildingWithMissingDataThrowsException()
        {
            EventStoreBuilder builder = new EventStoreBuilder();
            Assert.Throws<Exception>(() => builder.Build());
        }
    }
}
