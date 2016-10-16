using System;
using System.Linq;
using RabbitMQ.Client;

namespace SimpleEventStore
{
    public interface IEventPublisher
    {
        bool Publish(EventTransaction eventTransaction);
    }

    internal class RabbitMQEventPublisher : IEventPublisher
    {
        readonly RabbitMQConfiguration _configuration;
        readonly IConnectionFactory _connectionFactory;
        readonly IConnection _connection;
        readonly IModel _channel;

        IBinaryPublishedEventsSerializer Serializer { get { return _configuration.BinarySerializer; } }
        
        public RabbitMQEventPublisher(RabbitMQConfiguration configuration)
        {
            _configuration = configuration;
            _connectionFactory = new ConnectionFactory { HostName = configuration.HostName };
            _connection = _connectionFactory.CreateConnection();
            _channel = _connection.CreateModel();
            _channel.ExchangeDeclare(_configuration.ExchangeName, ExchangeType.Fanout, durable: false);
        }

        public bool Publish(EventTransaction eventTransactions)
        {
            try
            {
                var publishedEvents = new PublishedEvents();
                publishedEvents.Events = eventTransactions.Events.Select(c => new PublishedEvent
                {
                    AggregateId = eventTransactions.AggregateId,
                    SerialNumber = 0,
                    Event = c.SerializedEvent
                }).ToList();

                byte[] message = Serializer.Serialize(publishedEvents);

                    

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
    }

    internal class RabbitMQConfiguration
    {
        internal string HostName { get; private set; }
        internal string ExchangeName { get; private set; }
        internal IBinaryPublishedEventsSerializer BinarySerializer { get; private set; }

        public RabbitMQConfiguration(string hostName, string exchangeName, IBinaryPublishedEventsSerializer serializer)
        {
            HostName = hostName;
            ExchangeName = exchangeName;
            BinarySerializer = serializer;
        }
    }
}