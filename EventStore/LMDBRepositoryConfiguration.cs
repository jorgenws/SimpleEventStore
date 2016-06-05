namespace SimpleEventStore
{
    internal class LMDBRepositoryConfiguration
    {
        public int MaxDatabases { get; private set; }
        public long MapSize { get; private set; }
        public string EnvironmentPath { get; private set; }

        public LMDBRepositoryConfiguration(string environmentPath, int maxDatabases, long mapSize)
        {
            EnvironmentPath = environmentPath;
            MaxDatabases = maxDatabases;
            MapSize = mapSize;
        }

        //1073741824 = 1 GB
        public LMDBRepositoryConfiguration() : this(@"c:\lmdb", 2, 10485760 /*10MB*/) { }

        public LMDBRepositoryConfiguration(string environmentPath) : this(environmentPath, 2, 10485760 /*10MB*/) { }

        public LMDBRepositoryConfiguration(string environmentPath, long mapSize) : this(environmentPath, 2, mapSize) { }
    }
}
