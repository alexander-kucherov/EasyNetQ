using Microsoft.Extensions.Logging;

namespace EasyNetQ.Internals;

/// <summary>
///     This is an internal API that supports the EasyNetQ infrastructure and not subject to
///     the same compatibility as public APIs. It may be changed or removed without notice in
///     any release. You should only use it directly in your code with extreme caution and knowing that
///     doing so can result in application failures when updating to a new EasyNetQ release.
/// </summary>
public static class Timers
{
    /// <summary>
    ///     This is an internal API that supports the EasyNetQ infrastructure and not subject to
    ///     the same compatibility as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new EasyNetQ release.
    /// </summary>
    public static IDisposable Start(Action callback, TimeSpan dueTime, TimeSpan period, ILogger logger)
    {
        var callbackLock = new object();
        var timer = new Timer(_ =>
        {
            if (!Monitor.TryEnter(callbackLock)) return;
            try
            {
                callback.Invoke();
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "Error from timer callback");
            }
            finally
            {
                Monitor.Exit(callbackLock);
            }
        });
        timer.Change(dueTime, period);
        return timer;
    }

    public static IDisposable StartAsync(
        Func<CancellationToken, Task> callbackAsync,
        TimeSpan dueTime,
        TimeSpan period,
        ILogger logger)
    {
        return new AsyncTimerRunner(callbackAsync, dueTime, period, logger);
    }

    private sealed class AsyncTimerRunner : IDisposable
    {
        private readonly SemaphoreSlim mutex = new SemaphoreSlim(1, 1);
        private readonly CancellationTokenSource cts = new CancellationTokenSource();
        private readonly Func<CancellationToken, Task> callbackAsync;
        private readonly ILogger logger;
        private readonly Timer timer;
        private volatile int disposed;

        public AsyncTimerRunner(
            Func<CancellationToken, Task> callbackAsync,
            TimeSpan dueTime,
            TimeSpan period,
            ILogger logger)
        {
            this.callbackAsync = callbackAsync ?? throw new ArgumentNullException(nameof(callbackAsync));
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

            timer = new Timer(Callback, null, dueTime, period);
        }

        private async void Callback(object? state)
        {
            try
            {
                if (disposed == 1 || cts.IsCancellationRequested)
                    return;

                var entered = false;
                try
                {
                    entered = await mutex.WaitAsync(0);
                }
                catch (ObjectDisposedException)
                {
                    /* Semaphore has already been disposed */
                    return;
                }

                if (!entered) return;

                try
                {
                    if (disposed == 1 || cts.IsCancellationRequested)
                        return;

                    await callbackAsync(cts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cts.IsCancellationRequested) { /* */ }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Error from timer async callback");
                }
                finally
                {
                    try
                    {
                        mutex.Release();
                    }
                    catch (ObjectDisposedException) { /* Dispose race conditions */ }
                    catch (SemaphoreFullException) { /* Semaphore double release */ }
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Unhandled exception in timer async callback");
            }
        }

#pragma warning disable IDISP021
        public void Dispose()
        {
            if (Interlocked.Exchange(ref disposed, 1) == 1) return;

            try
            {
                timer.Change(Timeout.Infinite, Timeout.Infinite); 
                cts.Cancel();
                using var mre = new ManualResetEvent(false);
                timer.Dispose(mre);
                mre.WaitOne();
            }
            finally
            {
                mutex.Dispose();
                cts.Dispose();
            }
        }
    }
}
