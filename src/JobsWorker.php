<?php

declare(strict_types = 1);

namespace Spiral\RoadRunnerLaravel;

use Illuminate\Support\Carbon;
use Illuminate\Container\Container;
use Illuminate\Queue\WorkerOptions;
use Spiral\RoadRunner\Jobs\Consumer;
use Illuminate\Queue\Events\JobProcessed;
use Illuminate\Queue\Events\JobProcessing;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Spiral\RoadRunnerLaravel\Queue\RoadRunnerJob;
use Spiral\RoadRunnerLaravel\Application\Factory;
use Illuminate\Queue\Events\JobExceptionOccurred;
use Illuminate\Queue\MaxAttemptsExceededException;
use Illuminate\Contracts\Cache\Repository as CacheRepository;
use Illuminate\Contracts\Events\Dispatcher as EventDispatcher;
use Illuminate\Contracts\Foundation\Application as ApplicationContract;
use Spiral\RoadRunnerLaravel\Application\FactoryInterface as ApplicationFactory;

class JobsWorker implements WorkerInterface
{
    /**
     * @var CacheRepository
     */
    protected CacheRepository $cache;

    /**
     * @var EventDispatcher
     */
    protected EventDispatcher $events;

    /**
     * @var ExceptionHandler
     */
    protected ExceptionHandler $exceptions;

    /**
     * @var ApplicationFactory
     */
    protected ApplicationFactory $app_factory;

    /**
     * Jobs worker constructor.
     */
    public function __construct()
    {
        $this->app_factory = new Factory;
    }

    /**
     * {@inheritDoc}
     */
    public function start(WorkerOptionsInterface $options): void
    {
        /** @var Container $app */
        $app = $this->createApplication($options);

        $this->setCacheInstance($app);

        $this->setEventsInstance($app);

        /** @var ExceptionHandler $exception_handler */
        $exception_handler = $app->make(ExceptionHandler::class);

        $consumer = new Consumer;

        while ($task = $consumer->waitTask()) {

            $job     = new RoadRunnerJob($app, $task);
            $options = new WorkerOptions;

            try {
                $this->raiseBeforeJobEvent($job);
                $this->markJobAsFailedIfAlreadyExceedsMaxAttempts($job, $options->maxTries);

                if ($job->isDeleted()) {
                    $this->raiseAfterJobEvent($job);

                    return;
                }

                $job->fire();

                $this->raiseAfterJobEvent($job);
            } catch (\Throwable $e) {
                $exception_handler->report($e);
                $this->handleJobException($job, $options, $e);
            }
        }
    }

    /**
     * Raise the before queue job event.
     *
     * @param RoadRunnerJob $job
     *
     * @return void
     */
    protected function raiseBeforeJobEvent(RoadRunnerJob $job): void
    {
        $this->events->dispatch(new JobProcessing(
            $job->getConnectionName(), $job
        ));
    }

    /**
     * Raise the after queue job event.
     *
     * @param RoadRunnerJob $job
     *
     * @return void
     */
    protected function raiseAfterJobEvent(RoadRunnerJob $job): void
    {
        $this->events->dispatch(new JobProcessed(
            $job->getConnectionName(), $job
        ));
    }

    /**
     * Raise the exception occurred queue job event.
     *
     * @param RoadRunnerJob $job
     * @param \Throwable    $e
     */
    protected function raiseExceptionOccurredJobEvent(RoadRunnerJob $job, \Throwable $e): void
    {
        $this->events->dispatch(new JobExceptionOccurred($job->getConnectionName(), $job, $e));
    }

    /**
     * Mark the given job as failed if it has exceeded the maximum allowed attempts.
     *
     * This will likely be because the job previously exceeded a timeout.
     *
     * @param RoadRunnerJob $job
     * @param int           $max_tries
     *
     * @throws \Throwable
     */
    protected function markJobAsFailedIfAlreadyExceedsMaxAttempts(RoadRunnerJob $job, int $max_tries): void
    {
        $max_tries = $job->maxTries() ?? $max_tries;

        $retryUntil = $job->retryUntil();

        if ($retryUntil && Carbon::now()->getTimestamp() <= $retryUntil) {
            return;
        }

        if (! $retryUntil && ($max_tries === 0 || $job->attempts() <= $max_tries)) {
            return;
        }

        $this->failJob($job, $e = $this->maxAttemptsExceededException($job));

        throw $e;
    }

    /**
     * Mark the given job as failed and raise the relevant event.
     *
     * @param RoadRunnerJob $job
     * @param \Throwable    $e
     */
    protected function failJob(RoadRunnerJob $job, \Throwable $e): void
    {
        $job->fail($e);
    }

    /**
     * Create an instance of MaxAttemptsExceededException.
     *
     * @param RoadRunnerJob $job
     *
     * @return MaxAttemptsExceededException
     */
    protected function maxAttemptsExceededException(RoadRunnerJob $job): MaxAttemptsExceededException
    {
        return new MaxAttemptsExceededException(
            $job->resolveName()
            . ' has been attempted too many times or run too long. The job may have previously timed out.'
        );
    }

    /**
     * @param WorkerOptionsInterface $options
     *
     * @return ApplicationContract
     */
    protected function createApplication(WorkerOptionsInterface $options): ApplicationContract
    {
        return $this->app_factory->create($options->getAppBasePath());
    }

    /**
     * Handle an exception that occurred while the job was running.
     *
     * @param RoadRunnerJob $job
     * @param WorkerOptions $options
     * @param \Throwable    $e
     *
     * @throws \Throwable
     */
    protected function handleJobException(RoadRunnerJob $job, WorkerOptions $options, \Throwable $e): void
    {
        try {
            if (! $job->hasFailed()) {
                $this->markJobAsFailedIfWillExceedMaxAttempts($job, $options->maxTries, $e);
                $this->markJobAsFailedIfWillExceedMaxExceptions($job, $e);
            }

            $this->raiseExceptionOccurredJobEvent($job, $e);
        } finally {
            if (! $job->isDeleted() && ! $job->isReleased() && ! $job->hasFailed()) {
                $job->release($this->calculateBackoff($job, $options));
            }
        }

        throw $e;
    }

    /**
     * Mark the given job as failed if it has exceeded the maximum allowed attempts.
     *
     * @param RoadRunnerJob $job
     * @param int           $max_tries
     * @param \Throwable    $e
     */
    protected function markJobAsFailedIfWillExceedMaxAttempts(RoadRunnerJob $job, int $max_tries, \Throwable $e): void
    {
        $max_tries = $job->maxTries() ?? $max_tries;

        if ($job->retryUntil() && $job->retryUntil() <= Carbon::now()->getTimestamp()) {
            $this->failJob($job, $e);
        }

        if (! $job->retryUntil() && $max_tries > 0 && $job->attempts() >= $max_tries) {
            $this->failJob($job, $e);
        }
    }

    /**
     * Mark the given job as failed if it has exceeded the maximum allowed attempts.
     *
     * @param RoadRunnerJob $job
     * @param \Throwable    $e
     */
    protected function markJobAsFailedIfWillExceedMaxExceptions(RoadRunnerJob $job, \Throwable $e): void
    {
        if (($uuid = $job->uuid()) === null || ($max_exceptions = $job->maxExceptions()) === null) {
            return;
        }

        if (! $this->cache->get('job-exceptions:' . $uuid)) {
            $this->cache->put('job-exceptions:' . $uuid, 0, Carbon::now()->addDay());
        }

        if ($max_exceptions <= $this->cache->increment('job-exceptions:' . $uuid)) {
            $this->cache->forget('job-exceptions:' . $uuid);

            $this->failJob($job, $e);
        }
    }

    /**
     * Calculate the backoff for the given job.
     *
     * @param RoadRunnerJob $job
     * @param WorkerOptions $options
     *
     * @return int
     */
    protected function calculateBackoff(RoadRunnerJob $job, WorkerOptions $options): int
    {

        $backoff = method_exists($job, 'backoff') && $job->backoff() !== null
            ? $job->backoff()
            : $options->backoff;

        if (! \is_array($backoff)) {
            $backoff = explode(',', (string) $backoff);
        }

        return (int) ($backoff[$job->attempts() - 1] ?? last($backoff));// @phpstan-ignore-line
    }

    /**
     * @param Container $app
     *
     * @return void
     */
    private function setCacheInstance(Container $app): void
    {
        /** @var CacheRepository $cache */
        $cache = $app->make(CacheRepository::class);

        $this->cache = $cache;
    }

    /**
     * @param Container $app
     *
     * @return void
     */
    private function setEventsInstance(Container $app): void
    {
        /** @var EventDispatcher $events */
        $events = $app->make(EventDispatcher::class);

        $this->events = $events;
    }
}
