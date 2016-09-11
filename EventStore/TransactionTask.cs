using System;
using System.Threading.Tasks;

namespace SimpleEventStore
{
    internal class TransactionTask
    {
        public EventTransaction Transaction { get; private set; }

        public bool IsWritten { get; set; }
        public bool IsPublished { get; set; }

        private readonly TaskCompletionSource<bool> _tcs;

        public TransactionTask(EventTransaction transaction, TaskCompletionSource<bool> tcs)
        {
            Transaction = transaction;
            _tcs = tcs;
        }

        public void Finish()
        {
            _tcs.SetResult(IsWritten);
        }

        public void Finish(Exception exception)
        {
            _tcs.SetException(exception);
        }
    }
}
