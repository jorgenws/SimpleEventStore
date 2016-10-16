using System;
using System.Linq;
using RabbitMQ.Client;

namespace SimpleEventStore
{
    public interface IEventPublisher
    {
        bool Publish(EventTransaction eventTransaction);
    }

    public class DummyEventPublisher : IEventPublisher
    {
        public bool Publish(EventTransaction eventTransaction)
        {
            return true;
        }
    }

    internal class RabbitMQEventPublisher : IEventPublisher
    {
        readonly RabbitMQConfiguration _configuration;
        readonly IConnectionFactory _connectionFactory;

        IBinaryPublishedEventsSerializer Serializer { get { return _configuration.BinarySerializer; } }
        
        public RabbitMQEventPublisher(RabbitMQConfiguration configuration)
        {
            _configuration = configuration;
            _connectionFactory = new ConnectionFactory { HostName = configuration.HostName };
            CreateExchange();
        }

        public bool Publish(EventTransaction eventTransactions)
        {
            try
            {
                using (var connection = _connectionFactory.CreateConnection())
                using (var channel = connection.CreateModel())
                {
                    var publishedEvents = new PublishedEvents();
                    publishedEvents.Events = eventTransactions.Events.Select(c => new PublishedEvent
                    {
                        AggregateId = eventTransactions.AggregateId,
                        SerialNumber = 0,
                        Event = c.SerializedEvent
                    }).ToList();

                    byte[] message = Serializer.Serialize(publishedEvents);
                    channel.BasicPublish(exchange: _configuration.ExchangeName,
                                         routingKey: "",
                                         basicProperties: null,
                                         body: message);
                }
            }
            catch (Exception)
            {
                return false;
            }

            return true;
        }

        private void CreateExchange()
        {
            using (var conn = _connectionFactory.CreateConnection())
            using (var channel = conn.CreateModel())
                channel.ExchangeDeclare(_configuration.ExchangeName, ExchangeType.Fanout);
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