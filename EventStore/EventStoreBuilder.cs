using System;

namespace SimpleEventStore
{
    public class EventStoreBuilder : IEventStoreBuilder, ILMDBRepositoryBuilder, ISQLiteRepositoryBuilder, IEventPublisherBuilder, IRabbitMqConfigurationBuilder, IEventRepositoryBuild
    {
        private RepositoryType _selectedRepo;
        private SQLiteRepositoryConfiguration _sqliteRepoConfig;
        private LMDBRepositoryConfiguration _lmdbRepoConfig;
        
        private PublisherType _selectedPublisher;
        private RabbitMQConfiguration _rabbitMQConfiguration;

        public ILMDBRepositoryBuilder UseLMDBRepository()
        {
            _selectedRepo = RepositoryType.LMDB;
            return this;
        }

        public ISQLiteRepositoryBuilder UseSQLiteRepository()
        {
            _selectedRepo = RepositoryType.SQLite;
            return this;
        }

        public IEventPublisherBuilder Configuration(string connectionString)
        {
            _sqliteRepoConfig = new SQLiteRepositoryConfiguration(connectionString);
            return this;
        }

        public IEventPublisherBuilder Configuration(string environmentPath, int maxDatabases, long mapSize)
        {
            _lmdbRepoConfig = new LMDBRepositoryConfiguration(environmentPath, maxDatabases, mapSize);
            return this;
        }

        public IRabbitMqConfigurationBuilder UseRabbitMQ()
        {
            _selectedPublisher = PublisherType.RabbitMQ;
            return this;
        }

        public IEventRepositoryBuild Configuration(string hostName, string exchangeName, IBinaryPublishedEventsSerializer serializer)
        {
            _rabbitMQConfiguration = new RabbitMQConfiguration(hostName, exchangeName, serializer);
            return this;
        }

        public EventStore Build()
        {
            IEventRepository eventRepository = null;
            IEventPublisher eventPublisher = null;

            if (_selectedRepo == RepositoryType.LMDB && _lmdbRepoConfig != null)
                eventRepository = new LMDBEventRepository(_lmdbRepoConfig);
            else if (_selectedRepo == RepositoryType.SQLite && _sqliteRepoConfig != null)
                eventRepository = new SQLiteEventRepository(_sqliteRepoConfig);
            else
                throw new Exception("Missing data to build event repository");

            if (_selectedPublisher == PublisherType.RabbitMQ && _rabbitMQConfiguration != null)
                eventPublisher = new RabbitMQEventPublisher(_rabbitMQConfiguration);
            else
                throw new Exception("Missing data to build event publisher");

            return new EventStore(eventRepository, eventPublisher);
        }

        public void Clear()
        {
            _selectedRepo = RepositoryType.NotSelected;
            _sqliteRepoConfig = null;
            _lmdbRepoConfig = null;
        }

        private enum RepositoryType
        {
            NotSelected,
            LMDB,
            SQLite
        }

        private enum PublisherType
        {
            NotSelected,
            RabbitMQ
        }
    }

    public interface IEventStoreBuilder
    {
        ILMDBRepositoryBuilder UseLMDBRepository();
        ISQLiteRepositoryBuilder UseSQLiteRepository();
        void Clear();
    }

    public interface ILMDBRepositoryBuilder
    {
        IEventPublisherBuilder Configuration(string environmentPath, int maxDatabases, long mapSize);
    }

    public interface ISQLiteRepositoryBuilder
    {
        IEventPublisherBuilder Configuration(string connectionString);
    }

    public interface IEventPublisherBuilder
    {
        IRabbitMqConfigurationBuilder UseRabbitMQ();
    }

    public interface IRabbitMqConfigurationBuilder
    {
        IEventRepositoryBuild Configuration(string hostName, string exchangeName, IBinaryPublishedEventsSerializer serializer);
    }

    public interface IEventRepositoryBuild
    {
        EventStore Build();
    }
}
