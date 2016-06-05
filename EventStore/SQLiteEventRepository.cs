using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.IO;

namespace SimpleEventStore
{
    public class SQLiteEventRepository : IEventRepository
    {
        private const string aggregateIdField = "AggregateId";
        private const string aggregateIdParameter = "@aggregateId";
        private const string savedDateField = "SavedDate";
        private const string savedDateParameter = "@savedDate";
        private const string serializedEventField = "SerializedEvent";
        private const string serializedEventParameter = "@serializedEvent";
        private const string serialNumberField = "SerialNumber";
        private const string serialNumberParameter = "@serialNumber";

        private SQLiteConnection _connection;
        private SQLiteCommand _insertCommand;
 
        private int _nextSerialNumber;

        public SQLiteEventRepository(SQLiteRepositoryConfiguration configuration)
        {
            InitConnection(configuration.ConnectionString);
            InitSerialNumber();
            InitInsertMethod();
        }

        private void InitConnection(string connectionString)
        {
            _connection = new SQLiteConnection(connectionString);
            _connection.Open();

            using (var command = new SQLiteCommand(_connection))
            {
                command.CommandText = string.Format("CREATE TABLE IF NOT EXISTS Events ({0} TEXT(36), {1} DateTime, {2} BLOB, {3} INTEGER)",
                                                    aggregateIdField,
                                                    savedDateField,
                                                    serializedEventField,
                                                    serialNumberField);
                command.ExecuteNonQuery();
            }
        }

        private void InitInsertMethod()
        {
            _insertCommand = new SQLiteCommand(_connection);
            _insertCommand.CommandText = string.Format("INSERT INTO Events ({0}, {1}, {2}, {3}) VALUES ({4}, {5}, {6}, {7})",
                                                       aggregateIdField,
                                                       savedDateField,
                                                       serializedEventField,
                                                       serialNumberField,
                                                       aggregateIdParameter,
                                                       savedDateParameter,
                                                       serializedEventParameter,
                                                       serialNumberParameter);
            var eventIdParamter = new SQLiteParameter(aggregateIdParameter, DbType.StringFixedLength);
            var eventParameter = new SQLiteParameter(serializedEventParameter, DbType.Binary);
            var happendAt = new SQLiteParameter(savedDateParameter, DbType.DateTime);
            var serialNumber = new SQLiteParameter(serialNumberParameter, DbType.Int64);

            _insertCommand.Parameters.Add(eventIdParamter);
            _insertCommand.Parameters.Add(eventParameter);
            _insertCommand.Parameters.Add(happendAt);
            _insertCommand.Parameters.Add(serialNumber);
        }

        public bool WriteEvents(List<EventTransaction> eventTransactionBatch)
        {
            bool success = true;

            DateTime utcNow = DateTime.UtcNow;
            var command = _insertCommand;
            SQLiteTransaction transaction = _connection.BeginTransaction();

            try
            {
                foreach (EventTransaction eventTransaction in eventTransactionBatch)
                {
                    foreach (var @event in eventTransaction.Events)
                    {
                        command.Parameters[aggregateIdParameter].Value = eventTransaction.AggregateId.ToString("D");
                        command.Parameters[serializedEventParameter].Value = @event.SerializedEvent;
                        command.Parameters[savedDateParameter].Value = utcNow;
                        command.Parameters[serialNumberParameter].Value = _nextSerialNumber;
                        _nextSerialNumber++;
                        command.ExecuteNonQuery();
                    }
                }
                transaction.Commit();
            }
            catch (Exception e)
            {
                success = false;
            }
            finally
            {
                transaction.Dispose();
            }

            return success;
        }

        public void InitSerialNumber()
        {
            int nextSerialNumber;
            using (var command = new SQLiteCommand(_connection))
            {
                command.CommandText = string.Format("SELECT MAX({0}) FROM events", serialNumberField);
                var scalar = command.ExecuteScalar();

                if (scalar == null || !int.TryParse(scalar.ToString(), out nextSerialNumber))
                    nextSerialNumber = 0;
                else
                    nextSerialNumber++;
            }

            _nextSerialNumber = nextSerialNumber;
        }

        public Event[] GetEventsForAggregate(Guid aggregateId)
        {
            using (var command = new SQLiteCommand(_connection))
            {
                command.CommandText = string.Format("SELECT {0} FROM events WHERE {1} = {2} ORDER BY {3}",
                                                    serializedEventField,
                                                    aggregateIdField,
                                                    aggregateIdParameter,
                                                    serialNumberField);
                command.Parameters.Add(new SQLiteParameter(aggregateIdParameter, aggregateId.ToString("D")));
                return GetEvents(command);
            }
        }

        public Event[] GetEventsForAggregate(Guid aggregateId, int largerThan)
        {
            using (var command = new SQLiteCommand(_connection))
            {
                command.CommandText = string.Format("SELECT {0} FROM events WHERE {1} = {2} AND {3} > {4} ORDER BY {5}",
                                                    serializedEventField,
                                                    aggregateIdField,
                                                    aggregateIdParameter,
                                                    serialNumberField,
                                                    serialNumberParameter,
                                                    serialNumberField);
                command.Parameters.Add(new SQLiteParameter(aggregateIdParameter, aggregateId.ToString("D")));
                command.Parameters.Add(new SQLiteParameter(serialNumberParameter, largerThan));
                return GetEvents(command);
            }
        }

        public Event[] GetAllEvents(int @from, int to)
        {
            using (var command = new SQLiteCommand(_connection))
            {
                command.CommandText = string.Format("SELECT {0} FROM events WHERE {1} >= {2} AND {3} <= {4} ORDER BY {5}",
                                                    serializedEventField,
                                                    serialNumberField,
                                                    "@from",
                                                    serialNumberField,
                                                    "@to",
                                                    serialNumberField);
                command.Parameters.Add(new SQLiteParameter("@from", @from));
                command.Parameters.Add(new SQLiteParameter("@to", to));
                return GetEvents(command);
            }
        }

        private Event[] GetEvents(SQLiteCommand command)
        {
            var events = new List<Event>();
            var reader = command.ExecuteReader();

            while (reader.Read())
                events.Add(new Event { SerializedEvent = GetBytes(reader) });

            return events.ToArray();
        }

        private byte[] GetBytes(SQLiteDataReader reader)
        {
            const int ChunkSize = 2048;
            byte[] buffer = new byte[ChunkSize];
            long bytesRead;
            long fieldOffset = 0;
            using (MemoryStream stream = new MemoryStream())
            {
                while ((bytesRead = reader.GetBytes(0, fieldOffset, buffer, 0, buffer.Length)) > 0)
                {
                    stream.Write(buffer, 0, (int)bytesRead);
                    fieldOffset += bytesRead;
                }
                return stream.ToArray();
            }
        }
    }
}
