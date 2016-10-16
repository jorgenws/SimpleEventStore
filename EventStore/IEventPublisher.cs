using System;
using System.Linq;
using RabbitMQ.Client;

namespace SimpleEventStore
{
    public interface IEventPublisher
    {
        bool Publish(EventTransaction eventTransaction);
    }


}