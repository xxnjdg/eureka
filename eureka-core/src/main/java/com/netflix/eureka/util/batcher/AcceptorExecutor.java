package com.netflix.eureka.util.batcher;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.netflix.eureka.util.batcher.TaskProcessor.ProcessingResult;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.monitor.Monitors;
import com.netflix.servo.monitor.StatsTimer;
import com.netflix.servo.monitor.Timer;
import com.netflix.servo.stats.StatsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.eureka.Names.METRIC_REPLICATION_PREFIX;

/**
 * An active object with an internal thread accepting tasks from clients, and dispatching them to
 * workers in a pull based manner. Workers explicitly request an item or a batch of items whenever they are
 * available. This guarantees that data to be processed are always up to date, and no stale data processing is done.
 *
 * <h3>Task identification</h3>
 * Each task passed for processing has a corresponding task id. This id is used to remove duplicates (replace
 * older copies with newer ones).
 *
 * <h3>Re-processing</h3>
 * If data processing by a worker failed, and the failure is transient in nature, the worker will put back the
 * task(s) back to the {@link AcceptorExecutor}. This data will be merged with current workload, possibly discarded if
 * a newer version has been already received.
 *
 * @author Tomasz Bak
 */
class AcceptorExecutor<ID, T> {

    private static final Logger logger = LoggerFactory.getLogger(AcceptorExecutor.class);

    private final String id;
    private final int maxBufferSize;
    private final int maxBatchingSize;
    private final long maxBatchingDelay;

    private final AtomicBoolean isShutdown = new AtomicBoolean(false);

    // 接收任务的队列
    private final BlockingQueue<TaskHolder<ID, T>> acceptorQueue = new LinkedBlockingQueue<>();
    // 重试任务的队列
    private final BlockingDeque<TaskHolder<ID, T>> reprocessQueue = new LinkedBlockingDeque<>();
    // 后台接收者线程
    private final Thread acceptorThread;

    // 待处理任务容器
    private final Map<ID, TaskHolder<ID, T>> pendingTasks = new HashMap<>();
    // 处理中的队列
    private final Deque<ID> processingOrder = new LinkedList<>();

    // 单项队列请求的信号量
    private final Semaphore singleItemWorkRequests = new Semaphore(0);
    // 单项任务队列
    private final BlockingQueue<TaskHolder<ID, T>> singleItemWorkQueue = new LinkedBlockingQueue<>();

    // 批量队列请求的信号量
    private final Semaphore batchWorkRequests = new Semaphore(0);
    // 批量任务队列
    private final BlockingQueue<List<TaskHolder<ID, T>>> batchWorkQueue = new LinkedBlockingQueue<>();

    // 时间调整器
    private final TrafficShaper trafficShaper;

    /*
     * Metrics
     */
    @Monitor(name = METRIC_REPLICATION_PREFIX + "acceptedTasks", description = "Number of accepted tasks", type = DataSourceType.COUNTER)
    volatile long acceptedTasks;

    @Monitor(name = METRIC_REPLICATION_PREFIX + "replayedTasks", description = "Number of replayedTasks tasks", type = DataSourceType.COUNTER)
    volatile long replayedTasks;

    @Monitor(name = METRIC_REPLICATION_PREFIX + "expiredTasks", description = "Number of expired tasks", type = DataSourceType.COUNTER)
    volatile long expiredTasks;

    @Monitor(name = METRIC_REPLICATION_PREFIX + "overriddenTasks", description = "Number of overridden tasks", type = DataSourceType.COUNTER)
    volatile long overriddenTasks;

    @Monitor(name = METRIC_REPLICATION_PREFIX + "queueOverflows", description = "Number of queue overflows", type = DataSourceType.COUNTER)
    volatile long queueOverflows;

    private final Timer batchSizeMetric;

    AcceptorExecutor(String id,
                     int maxBufferSize,
                     int maxBatchingSize,
                     long maxBatchingDelay,
                     long congestionRetryDelayMs,
                     long networkFailureRetryMs) {
        this.id = id;
        this.maxBufferSize = maxBufferSize;
        this.maxBatchingSize = maxBatchingSize;
        this.maxBatchingDelay = maxBatchingDelay;
        // 时间调整器
        // congestionRetryDelayMs 阻塞重试延迟时间，1000ms
        // networkFailureRetryMs 网络异常重试时间，100ms
        this.trafficShaper = new TrafficShaper(congestionRetryDelayMs, networkFailureRetryMs);

        // 接收者后台处理线程
        ThreadGroup threadGroup = new ThreadGroup("eurekaTaskExecutors");
        this.acceptorThread = new Thread(threadGroup, new AcceptorRunner(), "TaskAcceptor-" + id);
        this.acceptorThread.setDaemon(true);
        this.acceptorThread.start();

        // 监控统计相关
        final double[] percentiles = {50.0, 95.0, 99.0, 99.5};
        final StatsConfig statsConfig = new StatsConfig.Builder()
                .withSampleSize(1000)
                .withPercentiles(percentiles)
                .withPublishStdDev(true)
                .build();
        final MonitorConfig config = MonitorConfig.builder(METRIC_REPLICATION_PREFIX + "batchSize").build();
        this.batchSizeMetric = new StatsTimer(config, statsConfig);
        try {
            Monitors.registerObject(id, this);
        } catch (Throwable e) {
            logger.warn("Cannot register servo monitor for this object", e);
        }
    }

    void process(ID id, T task, long expiryTime) {
        //将任务添加到接收者队列 acceptorQueue 的队尾了
        acceptorQueue.add(new TaskHolder<ID, T>(id, task, expiryTime));
        acceptedTasks++;
    }

    void reprocess(List<TaskHolder<ID, T>> holders, ProcessingResult processingResult) {
        // 添加到重处理队列 reprocessQueue
        reprocessQueue.addAll(holders);
        replayedTasks += holders.size();
        // 时间调整器注册失败
        trafficShaper.registerFailure(processingResult);
    }

    void reprocess(TaskHolder<ID, T> taskHolder, ProcessingResult processingResult) {
        reprocessQueue.add(taskHolder);
        replayedTasks++;
        trafficShaper.registerFailure(processingResult);
    }

    BlockingQueue<TaskHolder<ID, T>> requestWorkItem() {
        singleItemWorkRequests.release();
        return singleItemWorkQueue;
    }

    BlockingQueue<List<TaskHolder<ID, T>>> requestWorkItems() {
        batchWorkRequests.release();
        return batchWorkQueue;
    }

    void shutdown() {
        if (isShutdown.compareAndSet(false, true)) {
            Monitors.unregisterObject(id, this);
            acceptorThread.interrupt();
        }
    }

    @Monitor(name = METRIC_REPLICATION_PREFIX + "acceptorQueueSize", description = "Number of tasks waiting in the acceptor queue", type = DataSourceType.GAUGE)
    public long getAcceptorQueueSize() {
        return acceptorQueue.size();
    }

    @Monitor(name = METRIC_REPLICATION_PREFIX + "reprocessQueueSize", description = "Number of tasks waiting in the reprocess queue", type = DataSourceType.GAUGE)
    public long getReprocessQueueSize() {
        return reprocessQueue.size();
    }

    @Monitor(name = METRIC_REPLICATION_PREFIX + "queueSize", description = "Task queue size", type = DataSourceType.GAUGE)
    public long getQueueSize() {
        return pendingTasks.size();
    }

    @Monitor(name = METRIC_REPLICATION_PREFIX + "pendingJobRequests", description = "Number of worker threads awaiting job assignment", type = DataSourceType.GAUGE)
    public long getPendingJobRequests() {
        return singleItemWorkRequests.availablePermits() + batchWorkRequests.availablePermits();
    }

    @Monitor(name = METRIC_REPLICATION_PREFIX + "availableJobs", description = "Number of jobs ready to be taken by the workers", type = DataSourceType.GAUGE)
    public long workerTaskQueueSize() {
        return singleItemWorkQueue.size() + batchWorkQueue.size();
    }

    class AcceptorRunner implements Runnable {
        @Override
        public void run() {
            long scheduleTime = 0;
            while (!isShutdown.get()) {
                try {
                    // 排出输入队列的任务：将 reprocessQueue、acceptorQueue 队列的任务转移到 pendingTasks
                    drainInputQueues();

                    // 待处理的数量
                    int totalItems = processingOrder.size();

                    long now = System.currentTimeMillis();
                    if (scheduleTime < now) {
                        // 时间补偿，正常情况下 transmissionDelay() 返回 0
                        scheduleTime = now + trafficShaper.transmissionDelay();
                    }
                    if (scheduleTime <= now) {
                        // 分配批量工作任务：将 pendingTasks 的任务分一批到（最多250个） batchWorkQueue 队列中
                        assignBatchWork();
                        // 分配单项工作任务：pendingTasks 如果还有剩余任务，将没有过期的转移到 singleItemWorkQueue 队列中
                        assignSingleItemWork();
                    }

                    // If no worker is requesting data or there is a delay injected by the traffic shaper,
                    // sleep for some time to avoid tight loop.
                    if (totalItems == processingOrder.size()) {
                        Thread.sleep(10);
                    }
                } catch (InterruptedException ex) {
                    // Ignore
                } catch (Throwable e) {
                    // Safe-guard, so we never exit this loop in an uncontrolled way.
                    logger.warn("Discovery AcceptorThread error", e);
                }
            }
        }

        private boolean isFull() {
            // 待处理的任务 >= 10000，也就是说 pendingTasks 最多放 10000 个任务
            return pendingTasks.size() >= maxBufferSize;
        }

        private void drainInputQueues() throws InterruptedException {
            do {
                // 排出 reprocessQueue，将 reprocessQueue 队列的任务转移到 pendingTasks
                drainReprocessQueue();
                // 排出 acceptorQueue，将 acceptorQueue 队列的任务转移到 pendingTasks
                drainAcceptorQueue();

                if (isShutdown.get()) {
                    break;
                }
                // If all queues are empty, block for a while on the acceptor queue
                if (reprocessQueue.isEmpty() && acceptorQueue.isEmpty() && pendingTasks.isEmpty()) {
                    // 等待任务放入 acceptorQueue，等待 10 毫秒
                    TaskHolder<ID, T> taskHolder = acceptorQueue.poll(10, TimeUnit.MILLISECONDS);
                    if (taskHolder != null) {
                        // 放入之后 acceptorQueue、pendingTasks 就不为空了
                        appendTaskHolder(taskHolder);
                    }
                }
                // pendingTasks 为空、acceptorQueue 不为空、reprocessQueue不为空时，就会一直循环
                // 如果所有任务都处理完了，reprocessQueue、acceptorQueue、pendingTasks 都是空的，
                // 这时就会循环等待任务进入 acceptorQueue，每次等待 10 毫秒
            } while (!reprocessQueue.isEmpty() || !acceptorQueue.isEmpty() || pendingTasks.isEmpty());
        }

        private void drainAcceptorQueue() {
            while (!acceptorQueue.isEmpty()) {
                // 将 acceptorQueue 的任务转移到 pendingTasks
                appendTaskHolder(acceptorQueue.poll());
            }
        }

        private void drainReprocessQueue() {
            long now = System.currentTimeMillis();
            while (!reprocessQueue.isEmpty() && !isFull()) {
                // 从 reprocessQueue 队尾取出任务
                TaskHolder<ID, T> taskHolder = reprocessQueue.pollLast();
                ID id = taskHolder.getId();
                if (taskHolder.getExpiryTime() <= now) {
                    // 任务过期
                    expiredTasks++;
                } else if (pendingTasks.containsKey(id)) {
                    // pendingTasks 已存在
                    overriddenTasks++;
                } else {
                    // 将 reprocessQueue 队列的任务放到 pendingTasks
                    pendingTasks.put(id, taskHolder);
                    // 添加到 processingOrder 队列的头部，reprocessQueue 是失败重试的队列，所以优先级高一些
                    processingOrder.addFirst(id);
                }
            }
            if (isFull()) {
                queueOverflows += reprocessQueue.size();
                // pendingTasks 满了，就清空 reprocessQueue
                reprocessQueue.clear();
            }
        }

        private void appendTaskHolder(TaskHolder<ID, T> taskHolder) {
            if (isFull()) {
                // pendingTasks 满了就移除一个元素
                pendingTasks.remove(processingOrder.poll());
                queueOverflows++;
            }
            // 将 acceptorQueue 里的任务放到 pendingTasks
            TaskHolder<ID, T> previousTask = pendingTasks.put(taskHolder.getId(), taskHolder);
            if (previousTask == null) {
                // 原本不存在，将任务ID添加到 processingOrder 队列的最后
                processingOrder.add(taskHolder.getId());
            } else {
                // 已经存在了，就是覆盖
                overriddenTasks++;
            }
        }

        void assignSingleItemWork() {
            if (!processingOrder.isEmpty()) {
                if (singleItemWorkRequests.tryAcquire(1)) {
                    long now = System.currentTimeMillis();
                    while (!processingOrder.isEmpty()) {
                        ID id = processingOrder.poll();
                        TaskHolder<ID, T> holder = pendingTasks.remove(id);
                        if (holder.getExpiryTime() > now) {
                            // 将 pendingTasks 的任务移到 singleItemWorkQueue
                            singleItemWorkQueue.add(holder);
                            return;
                        }
                        expiredTasks++;
                    }
                    singleItemWorkRequests.release();
                }
            }
        }

        void assignBatchWork() {
            // 有足够的任务做一个批处理
            if (hasEnoughTasksForNextBatch()) {
                if (batchWorkRequests.tryAcquire(1)) {
                    long now = System.currentTimeMillis();
                    // 一批任务最多 250 个
                    int len = Math.min(maxBatchingSize, processingOrder.size());
                    List<TaskHolder<ID, T>> holders = new ArrayList<>(len);
                    // 将 pendingTasks 中的任务移动一批到 holders 中
                    // 也就是说，如果队列中有500个任务，这一批任务最多也是250个
                    while (holders.size() < len && !processingOrder.isEmpty()) {
                        ID id = processingOrder.poll();
                        TaskHolder<ID, T> holder = pendingTasks.remove(id);
                        if (holder.getExpiryTime() > now) {
                            holders.add(holder);
                        } else {
                            expiredTasks++;
                        }
                    }
                    if (holders.isEmpty()) {
                        batchWorkRequests.release();
                    } else {
                        batchSizeMetric.record(holders.size(), TimeUnit.MILLISECONDS);
                        // 添加到批量队列中
                        batchWorkQueue.add(holders);
                    }
                }
            }
        }

        // 是否有足够的任务做一个批处理
        private boolean hasEnoughTasksForNextBatch() {
            if (processingOrder.isEmpty()) {
                return false;
            }
            if (pendingTasks.size() >= maxBufferSize) {
                return true;
            }

            // 从 processingOrder 队首取一个任务ID，然后从 pendingTasks 读取这个任务。注意 peek() 只是取出元素，并不会移除队首的元素
            TaskHolder<ID, T> nextHolder = pendingTasks.get(processingOrder.peek());
            // 判断任务提交到现在的时间差是否超过最大批任务延迟时间（500毫秒）
            long delay = System.currentTimeMillis() - nextHolder.getSubmitTimestamp();
            return delay >= maxBatchingDelay;
        }
    }
}
