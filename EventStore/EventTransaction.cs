using System;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleEventStore
{
    public class EventTransaction
    {
        public Guid AggregateId { get; set; }
        public Event[] Events { get; set; }

        public bool IsWriteSuccess { get; set; }
        public bool IsPublishSuccess { get; set; }
        public bool IsFinished { get; set; }

        private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(0, 1);

        public async Task WaitAsync()
        {
            await _semaphore.WaitAsync();
        }

        public void Finished(bool writeSuccess, bool publishSuccess)
        {
            IsFinished = true;
            IsWriteSuccess = writeSuccess;
            IsPublishSuccess = publishSuccess;
            _semaphore.Release();
        }
    }
}
