using System;

namespace SimpleEventStore
{
    public class EventStoreBuilder : IEventStoreBuilder, ILMDBRepositoryBuilder, ISQLiteRepositoryBuilder, IEventPublisherBuilder, IRabbitMqConfigurationBuilder, IEventRepositoryBuild
    {
        private RepositoryType _selectedRepo;
        private SQLiteRepositoryConfiguration _sqliteRepoConfig;
        private LMDBRepositoryConfiguration _lmdbRepoConfig;
        private IEventRepository _customRepository;
        
        private PublisherType _selectedPublisher;
        private RabbitMQConfiguration _rabbitMQConfiguration;
        private IEventPublisher _customPublisher;

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

        public IEventPublisherBuilder UseCustom(IEventRepository repository)
        {
            _selectedRepo = RepositoryType.Custom;
            _customRepository = repository;
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

        public IEventRepositoryBuild UseCustom(IEventPublisher publisher)
        {
            _selectedPublisher = PublisherType.Custom;
            _customPublisher = publisher;
            return this;
        }

        public IEventRepositoryBuild UseDummyPublisher()
        {
            _selectedPublisher = PublisherType.Dummy;
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
            else if (_selectedRepo == RepositoryType.Custom)
                eventRepository = _customRepository;
            else
                throw new Exception("Missing data to build event repository");

            if (_selectedPublisher == PublisherType.RabbitMQ && _rabbitMQConfiguration != null)
                eventPublisher = new RabbitMQEventPublisher(_rabbitMQConfiguration);
            else if (_selectedPublisher == PublisherType.Dummy)
                eventPublisher = new DummyEventPublisher();
            else if (_selectedPublisher == PublisherType.Custom)
                eventPublisher = _customPublisher;
            else
                throw new Exception("Missing data to build event publisher");

            return new EventStore(eventRepository, eventPublisher);
        }

        public void Clear()
        {
            _selectedRepo = RepositoryType.NotSelected;
            _selectedPublisher = PublisherType.NotSelected;
            _sqliteRepoConfig = null;
            _lmdbRepoConfig = null;
            _customRepository = null;
            _customPublisher = null;
        }

        private enum RepositoryType
        {
            NotSelected,
            LMDB,
            SQLite,
            Custom
        }

        private enum PublisherType
        {
            NotSelected,
            RabbitMQ,
            Dummy,
            Custom
        }
    }

    public interface IEventStoreBuilder
    {
        ILMDBRepositoryBuilder UseLMDBRepository();
        ISQLiteRepositoryBuilder UseSQLiteRepository();
        IEventPublisherBuilder UseCustom(IEventRepository repository);
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
        IEventRepositoryBuild UseCustom(IEventPublisher publisher);
        IEventRepositoryBuild UseDummyPublisher();
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
