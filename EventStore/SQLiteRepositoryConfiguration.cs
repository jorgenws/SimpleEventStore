namespace SimpleEventStore
{
    public class SQLiteRepositoryConfiguration
    {
        public string ConnectionString { get; private set; }

        public SQLiteRepositoryConfiguration(string connectionString)
        {
            ConnectionString = connectionString;
        }
    }
}