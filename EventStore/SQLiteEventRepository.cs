using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Text;

namespace EventStore
{
    public class SQLiteEventRepository : IEventRepository
    {
        private SQLiteConnection _connection;
        private SQLiteConnection Connection
        {
            get
            {
                if (_connection == null)
                {
                    _connection = new SQLiteConnection("Data Source=:memory:");
                    //_connection = new SQLiteConnection("Data Source=test.db");
                    _connection.Open();

                    using (var command = new SQLiteCommand(_connection))
                    {
                        command.CommandText = "CREATE TABLE IF NOT EXISTS Events (Id TEXT(36), UserId TEXT(36), CompanyId TEXT(36), HappendAt DateTime,  SerializedEvent BLOB, SerialNumber INTEGER)";
                        command.ExecuteNonQuery();
                    }
                }

                return _connection;
            }
        }

        private SQLiteCommand _insertCommand;
        private SQLiteCommand InsertCommand
        {
            get
            {
                if(_insertCommand == null)
                {
                    _insertCommand = new SQLiteCommand("INSERT INTO Events (Id, UserId, CompanyId, HappendAt, SerializedEvent, SerialNumber) VALUES (@id, @userId, @companyId, @happendAt, @serializedEvent, @serialNumber)", Connection);
                    var userIdParameter = new SQLiteParameter("@userId", DbType.StringFixedLength);
                    var companyIdParameter = new SQLiteParameter("@companyId", DbType.StringFixedLength);
                    var eventIdParamter = new SQLiteParameter("@id", DbType.StringFixedLength);
                    var eventParameter = new SQLiteParameter("@serializedEvent", DbType.Binary);
                    var happendAt = new SQLiteParameter("@happendAt", DbType.DateTime);
                    var serialNumber = new SQLiteParameter("@serialNumber", DbType.Int64);

                    _insertCommand.Parameters.Add(userIdParameter);
                    _insertCommand.Parameters.Add(companyIdParameter);
                    _insertCommand.Parameters.Add(eventIdParamter);
                    _insertCommand.Parameters.Add(eventParameter);
                    _insertCommand.Parameters.Add(happendAt);
                    _insertCommand.Parameters.Add(serialNumber);
                }

                return _insertCommand;
            }
        }

        private int _nextSerialNumber;

        public SQLiteEventRepository()
        {
             _nextSerialNumber = NextSerialNumber();
        }

        public bool WriteEvents(List<EventTransaction> eventTransactionBatch)
        {
            bool success = true;

            DateTime utcNow = DateTime.UtcNow;
            var command = InsertCommand;
            SQLiteTransaction transaction = Connection.BeginTransaction();

            try
            {
                foreach (var eventTransaction in eventTransactionBatch)
                {
                    foreach (var @event in eventTransaction.Events)
                    {
                        command.Parameters["@userId"].Value = eventTransaction.UserId.ToString("D");
                        command.Parameters["@companyId"].Value =  eventTransaction.CompanyId.ToString("D");
                        command.Parameters["@id"].Value = @event.Id.ToString("D");
                        command.Parameters["@serializedEvent"].Value = Encoding.UTF8.GetBytes(@event.SerializedEvent);
                        command.Parameters["@happendAt"].Value = utcNow;
                        command.Parameters["@serialNumber"].Value = _nextSerialNumber;
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

        public int NextSerialNumber()
        {
            int nextSerialNumber;
            using (var command = new SQLiteCommand(Connection))
            {
                command.CommandText = "SELECT MAX(serialNumber) FROM events";
                var scalar = command.ExecuteScalar();

                if (scalar == null || !int.TryParse(scalar.ToString(), out nextSerialNumber))
                    nextSerialNumber = 1;
                else
                    nextSerialNumber++;
            }

            return nextSerialNumber;
        }
    }
}
