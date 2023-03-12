<?php

declare(strict_types = 1);

namespace Spiral\RoadRunnerLaravel\Queue;

use Illuminate\Queue\Jobs\Job;
use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Job as JobContract;
use Spiral\RoadRunner\Jobs\Task\ReceivedTaskInterface;

/**
 * Job decorator.
 */
class RoadRunnerJob extends Job implements JobContract
{
    /**
     * Job attempts header name.
     */
    private const         JOB_ATTEMPTS = 'job-attempts';

    /**
     * @param Container             $container
     * @param ReceivedTaskInterface $task
     */
    public function __construct(Container $container, private ReceivedTaskInterface $task)
    {
        $this->container = $container;
    }

    /**
     * {@inheritDoc}
     */
    public function getJobId(): string
    {
        return $this->task->getId();
    }

    /**
     * {@inheritDoc}
     */
    public function getQueue(): string
    {
        return $this->task->getQueue();
    }

    /**
     * {@inheritDoc}
     */
    public function getName(): string
    {
        return $this->task->getName();
    }

    /**
     * {@inheritDoc}
     */
    public function getRawBody(): string
    {
        return \json_encode($this->payload(), \JSON_THROW_ON_ERROR);
    }

    /**
     * @return array<mixed>
     *
     * {@inheritDoc}
     */
    public function payload(): array
    {
        return $this->task->getPayload();
    }

    /**
     * {@inheritDoc}
     */
    public function attempts(): int
    {
        $attempt = $this->task->getHeaderLine(self::JOB_ATTEMPTS);

        return $attempt === ''
            ? 1
            : (int) $attempt;
    }

    /**
     * @return void
     */
    public function fire(): void
    {
        parent::fire();

        $this->task->complete();
    }

    /**
     * {@inheritDoc}
     */
    public function release($delay = 0): void
    {
        parent::release($delay);

        $this->setAttempts($this->attempts() + 1);

        if ($delay > 0) {
            $this->task = $this->task->withDelay($delay);
        }

        $this->task->fail('Release message back to queue', true);
    }

    /**
     * @param int $attempts
     */
    private function setAttempts(int $attempts): void
    {
        $this->task = $this->task->withHeader(self::JOB_ATTEMPTS, (string) $attempts);
    }

    /**
     * {@inheritDoc}
     */
    protected function failed($e): void
    {
        parent::failed($e);

        $this->task->fail($e ?? 'Job was failed');
    }
}
