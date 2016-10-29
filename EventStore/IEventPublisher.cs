using Events;
using System;

namespace SimpleEventStore
{
    public interface IEventPublisher : IDisposable
    {
        bool Publish(EventTransaction eventTransaction);
    }
}