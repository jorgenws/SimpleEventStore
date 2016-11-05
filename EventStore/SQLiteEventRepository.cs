using Events;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.IO;

namespace SimpleEventStore
{
    internal class SQLiteEventRepository : IEventRepository
    {
        private const string aggregateIdField = "AggregateId";
        private const string aggregateIdParameter = "@aggregateId";
        private const string savedDateField = "SavedDate";
        private const string savedDateParameter = "@savedDate";
        private const string typeOfEventField = "TypeOfEvent";
        private const string typeOfEventParameter = "@typeOfEvent";
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
                //Index slows down inserts and the slow down increases with the size of the table.
                command.CommandText = string.Format("BEGIN;"+
                                                    "CREATE TABLE IF NOT EXISTS Events ({0} TEXT(36) NOT NULL, {1} DateTime NOT NULL, {2} TEXT NOT NULL, {3} BLOB NOT NULL, {4} INTEGER NOT NULL);"+
                                                    "CREATE INDEX IF NOT EXISTS serialIdIndex ON Events ({4});"+
                                                    "COMMIT;",
                                                    aggregateIdField,
                                                    savedDateField,
                                                    typeOfEventField,
                                                    serializedEventField,
                                                    serialNumberField);
                command.ExecuteNonQuery();
            }
        }

        private void InitInsertMethod()
        {
            _insertCommand = new SQLiteCommand(_connection);
            _insertCommand.CommandText = string.Format("INSERT INTO Events ({0}, {1}, {2}, {3}, {4}) VALUES ({5}, {6}, {7}, {8}, {9})",
                                                       aggregateIdField,
                                                       savedDateField,
                                                       serializedEventField,
                                                       serialNumberField,
                                                       typeOfEventField,
                                                       aggregateIdParameter,
                                                       savedDateParameter,
                                                       serializedEventParameter,
                                                       serialNumberParameter,
                                                       typeOfEventParameter);
            var eventIdParamter = new SQLiteParameter(aggregateIdParameter, DbType.StringFixedLength);
            var eventParameter = new SQLiteParameter(serializedEventParameter, DbType.Binary);
            var happendAt = new SQLiteParameter(savedDateParameter, DbType.DateTime);
            var serialNumber = new SQLiteParameter(serialNumberParameter, DbType.Int64);
            var typeOfEvent = new SQLiteParameter(typeOfEventParameter, DbType.String);

            _insertCommand.Parameters.Add(eventIdParamter);
            _insertCommand.Parameters.Add(eventParameter);
            _insertCommand.Parameters.Add(happendAt);
            _insertCommand.Parameters.Add(serialNumber);
            _insertCommand.Parameters.Add(typeOfEvent);
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
                        command.Transaction = transaction;
                        command.Parameters[aggregateIdParameter].Value = @event.AggregateId.ToString("D");
                        command.Parameters[serializedEventParameter].Value = @event.SerializedEvent;
                        command.Parameters[savedDateParameter].Value = utcNow;
                        command.Parameters[serialNumberParameter].Value = @event.SerialId;
                        command.Parameters[typeOfEventParameter].Value = @event.EventType;
                        command.ExecuteNonQuery();
                    }
                }
                transaction.Commit();
            }
            catch (Exception)
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
                command.CommandText = string.Format("SELECT {0}, {1}, {2}, {3} FROM events WHERE {0} = {4} ORDER BY {1}",
                                                    aggregateIdField,
                                                    serialNumberField,
                                                    typeOfEventField,
                                                    serializedEventField,
                                                    aggregateIdParameter);
                command.Parameters.Add(new SQLiteParameter(aggregateIdParameter, aggregateId.ToString("D")));
                return GetEvents(command);
            }
        }

        public Event[] GetEventsForAggregate(Guid aggregateId, int largerThan)
        {
            using (var command = new SQLiteCommand(_connection))
            {
                command.CommandText = string.Format("SELECT {0}, {1}, {2}, {3} FROM events WHERE {0} = {4} AND {1} > {5} ORDER BY {1}",
                                                    aggregateIdField,
                                                    serialNumberField,
                                                    typeOfEventField,
                                                    serializedEventField,
                                                    aggregateIdParameter,
                                                    serialNumberParameter);
                command.Parameters.Add(new SQLiteParameter(aggregateIdParameter, aggregateId.ToString("D")));
                command.Parameters.Add(new SQLiteParameter(serialNumberParameter, largerThan));
                return GetEvents(command);
            }
        }

        public Event[] GetAllEvents(int @from, int to)
        {
            using (var command = new SQLiteCommand(_connection))
            {
                command.CommandText = string.Format("SELECT {0}, {1}, {2}, {3} FROM events WHERE {1} >= {4} AND {1} <= {5} ORDER BY {1}",
                                                    aggregateIdField,
                                                    serialNumberField,
                                                    typeOfEventField,
                                                    serializedEventField,
                                                    "@from",
                                                    "@to");
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
            {
                var e = new Event();
                    e.AggregateId = Guid.Parse(reader.GetString(0));
                e.SerialId = reader.GetInt32(1);
                e.EventType = reader.GetString(2);
                e.SerializedEvent = GetBytes(reader, 3);

                events.Add(e);
            }

            return events.ToArray();
        }

        private byte[] GetBytes(SQLiteDataReader reader, int columnIndex)
        {
            const int ChunkSize = 2048;
            byte[] buffer = new byte[ChunkSize];
            long bytesRead;
            long fieldOffset = 0;
            using (MemoryStream stream = new MemoryStream())
            {
                while ((bytesRead = reader.GetBytes(columnIndex, fieldOffset, buffer, 0, buffer.Length)) > 0)
                {
                    stream.Write(buffer, 0, (int)bytesRead);
                    fieldOffset += bytesRead;
                }
                return stream.ToArray();
            }
        }

        public void Dispose()
        {
            _insertCommand.Dispose();
            _connection.Dispose();
        }

        public int NextSerialNumber()
        {
            return _nextSerialNumber++;
        }

        public void ResetSerialNumber()
        {
            InitSerialNumber();
        }
    }
}