using System;

namespace SimpleEventStore
{
    public class EventStoreBuilder : IEventStoreBuilder, ILMDBRepositoryBuilder, ISQLiteRepositoryBuilder, IEventRepositoryBuild
    {
        private RepositoryType _selectedRepo;
        private SQLiteRepositoryConfiguration _sqliteRepoConfig;
        private LMDBRepositoryConfiguration _lmdbRepoConfig;

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

        public IEventRepositoryBuild Configuration(string connectionString)
        {
            _sqliteRepoConfig = new SQLiteRepositoryConfiguration(connectionString);
            return this;
        }

        public IEventRepositoryBuild Configuration(string environmentPath, int maxDatabases, long mapSize)
        {
            _lmdbRepoConfig = new LMDBRepositoryConfiguration(environmentPath, maxDatabases, mapSize);
            return this;
        }

        public EventStore Build()
        {
            EventStore es;

            if (_selectedRepo == RepositoryType.LMDB && _lmdbRepoConfig != null)
                es = new EventStore(new LMDBEventRepository(_lmdbRepoConfig));
            else if (_selectedRepo == RepositoryType.SQLite && _sqliteRepoConfig != null)
                es = new EventStore(new SQLiteEventRepository(_sqliteRepoConfig));
            else
                throw new Exception("Not a valid combination");

            return es;
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
    }

    public interface IEventStoreBuilder
    {
        ILMDBRepositoryBuilder UseLMDBRepository();
        ISQLiteRepositoryBuilder UseSQLiteRepository();
        void Clear();
    }

    public interface ILMDBRepositoryBuilder
    {
        IEventRepositoryBuild Configuration(string environmentPath, int maxDatabases, long mapSize);
    }

    public interface ISQLiteRepositoryBuilder
    {
        IEventRepositoryBuild Configuration(string connectionString);
    }

    public interface IEventRepositoryBuild
    {
        EventStore Build();
    }
}
