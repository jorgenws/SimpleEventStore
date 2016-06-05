using System.Collections.Generic;

namespace SimpleEventStore
{
    public interface IEventPublisher
    {
        bool Publish(List<EventTransaction> eventTransactions);
    }

    internal class RabbitMQEventPublisher : IEventPublisher
    {
        public bool Publish(List<EventTransaction> eventTransactions)
        {
            //Todo: do
            throw new System.NotImplementedException();
        }
    }
}