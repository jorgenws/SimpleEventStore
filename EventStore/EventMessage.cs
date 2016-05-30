using System;
using System.Threading;
using System.Threading.Tasks;

namespace EventStore
{
    public class EventTransaction
    {
        public Guid AggregateId { get; set; }
        public Event[] Events { get; set; }

        private SemaphoreSlim _semaphore = new SemaphoreSlim(0, 1);

        public async Task WaitAsync()
        {
            await _semaphore.WaitAsync();
        }

        public void HasFinished()
        {
            _semaphore.Release();
        }
    }
}
