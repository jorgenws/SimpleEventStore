namespace SimpleEventStore
{
    internal class LMDBRepositoryConfiguration
    {
        public int MaxDatabases { get; private set; }
        public long MapSize { get; private set; }
        public string EnvironmentPath { get; private set; }
        public IBinaryEventsSerializer Serializer { get; private set; }

        public LMDBRepositoryConfiguration(string environmentPath, int maxDatabases, long mapSize, IBinaryEventsSerializer serializer)
        {
            EnvironmentPath = environmentPath;
            MaxDatabases = maxDatabases;
            MapSize = mapSize;
            Serializer = serializer;
        }

        //1073741824 = 1 GB
        public LMDBRepositoryConfiguration(IBinaryEventsSerializer serializer) : this(@"c:\lmdb", 2, 10485760 /*10MB*/, serializer) { }

        public LMDBRepositoryConfiguration(string environmentPath, IBinaryEventsSerializer serializer) : this(environmentPath, 2, 10485760 /*10MB*/, serializer) { }

        public LMDBRepositoryConfiguration(string environmentPath, long mapSize, IBinaryEventsSerializer serializer) : this(environmentPath, 2, mapSize, serializer) { }
    }
}