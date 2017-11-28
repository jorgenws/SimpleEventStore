using Events;
using EventSerialization;
using RabbitMQ.Client;
using System;

namespace SimpleEventStore
{
    internal class RabbitMQEventPublisher : IEventPublisher
    {
        readonly RabbitMQConfiguration _configuration;
        readonly IConnectionFactory _connectionFactory;
        IConnection _connection;
        IModel _channel;

        IBinaryEventsSerializer Serializer { get { return _configuration.BinarySerializer; } }

        public RabbitMQEventPublisher(RabbitMQConfiguration configuration)
        {
            _configuration = configuration;
            _connectionFactory = new ConnectionFactory { HostName = configuration.HostName };
            SetUp();
        }

        private void SetUp()
        {
            _connection = _connectionFactory.CreateConnection();
            _connection.ConnectionShutdown += ConnectionShutdown;
            _channel = _connection.CreateModel();
            _channel.ModelShutdown += ModelShutDown;
            _channel.ExchangeDeclare(_configuration.ExchangeName, ExchangeType.Fanout, durable: false);
        }

        private void ConnectionShutdown(object sender, ShutdownEventArgs args)
        {
            CleanUp();
            SetUp();
        }

        private void ModelShutDown(object sender, ShutdownEventArgs e)
        {
            CleanUp();
            SetUp();
        }

        private void CleanUp()
        {
            if (_channel != null)
            {
                _channel.ModelShutdown -= ModelShutDown;
                _channel.Close();
                _channel.Dispose();
            }
            if(_connection != null)
            {
                _connection.ConnectionShutdown -= ConnectionShutdown;
                _connection.Close();
                _connection.Dispose();
            }
        }

        public bool Publish(EventTransaction eventTransactions)
        {
            try
            {
                byte[] message = Serializer.Serialize(eventTransactions);
                _channel.BasicPublish(exchange: _configuration.ExchangeName,
                                        routingKey: "",
                                        basicProperties: null,
                                        body: message);
            }
            catch (Exception)
            {
                return false;
            }

            return true;
        }

        public void Dispose()
        {
            CleanUp();
        }
    }

    internal class RabbitMQConfiguration
    {
        internal string HostName { get; private set; }
        internal string ExchangeName { get; private set; }
        internal IBinaryEventsSerializer BinarySerializer { get; private set; }

        public RabbitMQConfiguration(string hostName, string exchangeName, IBinaryEventsSerializer serializer)
        {
            HostName = hostName;
            ExchangeName = exchangeName;
            BinarySerializer = serializer;
        }
    }
}
