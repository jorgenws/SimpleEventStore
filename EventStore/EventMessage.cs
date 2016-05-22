using System;
using System.Threading;
using System.Threading.Tasks;

namespace EventStore
{
    public class EventTransaction
    {
        public Guid UserId { get; set; }
        public Guid CompanyId { get; set; }
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

    public class Event
    {
        public Guid Id { get; set; }
        public string SerializedEvent { get; set; }
    }
}
