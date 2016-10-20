using System;
using System.Threading.Tasks;

namespace SimpleEventStore
{
    internal class TransactionTask
    {
        public EventTransaction Transaction { get; private set; }

        private readonly TaskCompletionSource<bool> _tcs;

        public TransactionTask(EventTransaction transaction, TaskCompletionSource<bool> tcs)
        {
            Transaction = transaction;
            _tcs = tcs;
        }

        public void Finish(bool isPersisted)
        {
            _tcs.SetResult(isPersisted);
        }

        public void Finish(Exception exception)
        {
            _tcs.SetException(exception);
        }
    }
}
