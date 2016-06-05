using System;
using SimpleEventStore;
using NUnit.Framework;

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
                                                  .Build());

            Assert.NotNull(es);
        }

        [Test]
        public void BuildLMDBEventStore()
        {
            IEventStoreBuilder builder = new EventStoreBuilder();

            EventStore es = null;
            Assert.DoesNotThrow(() => es = builder.UseLMDBRepository()
                                                  .Configuration(@"c:\lmdb", 2, 10485760)
                                                  .Build());

            Assert.NotNull(es);
        }

        [Test]
        public void BuildingWithMissingDataThrowsException()
        {
            EventStoreBuilder builder = new EventStoreBuilder();
            Assert.Throws<Exception>(() => builder.Build());
        }
    }
}
