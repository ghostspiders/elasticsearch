/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 任何主节点选举算法都不可能保证能够选举出领导者，但只要选举发生的频率足够低，
 与向另一个节点发送消息并接收响应所需的时间相比，它们通常都能正常工作（随着时间的推移，成功的概率趋近于1）。
 我们不知道往返延迟是多少，但我们可以通过以合理的高频率尝试选举，并在失败时（线性地）退避，直到选举成功来进行近似。
 我们还对退避设置了上限，以便如果选举由于持续很长时间的网络分区而失败，那么当分区恢复时，可以尽快尝试选举。
 */
public class ElectionSchedulerFactory {


    /**
     * 日志记录器，用于记录与ElectionSchedulerFactory相关的日志信息。
     */
    private static final Logger logger = LogManager.getLogger(ElectionSchedulerFactory.class);

    /**
     * 选举初始超时设置的键。
     */
    private static final String ELECTION_INITIAL_TIMEOUT_SETTING_KEY = "cluster.election.initial_timeout";
    /**
     * 选举退避时间设置的键。
     */
    private static final String ELECTION_BACK_OFF_TIME_SETTING_KEY = "cluster.election.back_off_time";
    /**
     * 选举最大超时设置的键。
     */
    private static final String ELECTION_MAX_TIMEOUT_SETTING_KEY = "cluster.election.max_timeout";
    /**
     * 选举持续时间设置的键。
     */
    private static final String ELECTION_DURATION_SETTING_KEY = "cluster.election.duration";

    /*
     * 第一次选举被安排在调度器启动后的一个随机时间点，这个随机时间点是从以下范围内均匀选择的：
     *
     *     (0, min(ELECTION_INITIAL_TIMEOUT_SETTING, ELECTION_MAX_TIMEOUT_SETTING)]
     *
     * 对于`n > 1`，第`n`次选举被安排在第`n - 1`次选举之后的一个随机时间点，这个随机时间点是从以下范围内均匀选择的：
     *
     *     (0, min(ELECTION_INITIAL_TIMEOUT_SETTING + (n-1) * ELECTION_BACK_OFF_TIME_SETTING, ELECTION_MAX_TIMEOUT_SETTING)]
     *
     * 每次选举持续时间最多为ELECTION_DURATION_SETTING。
     */

    /**
     * 选举初始超时设置。
     */
    public static final Setting<TimeValue> ELECTION_INITIAL_TIMEOUT_SETTING = Setting.timeSetting(ELECTION_INITIAL_TIMEOUT_SETTING_KEY,
        TimeValue.timeValueMillis(100), TimeValue.timeValueMillis(1), TimeValue.timeValueSeconds(10), Property.NodeScope);

    /**
     * 选举退避时间设置。
     */
    public static final Setting<TimeValue> ELECTION_BACK_OFF_TIME_SETTING = Setting.timeSetting(ELECTION_BACK_OFF_TIME_SETTING_KEY,
        TimeValue.timeValueMillis(100), TimeValue.timeValueMillis(1), TimeValue.timeValueSeconds(60), Property.NodeScope);

    /**
     * 选举最大超时设置。
     */
    public static final Setting<TimeValue> ELECTION_MAX_TIMEOUT_SETTING = Setting.timeSetting(ELECTION_MAX_TIMEOUT_SETTING_KEY,
        TimeValue.timeValueSeconds(10), TimeValue.timeValueMillis(200), TimeValue.timeValueSeconds(300), Property.NodeScope);

    /**
     * 选举持续时间设置。
     */
    public static final Setting<TimeValue> ELECTION_DURATION_SETTING = Setting.timeSetting(ELECTION_DURATION_SETTING_KEY,
        TimeValue.timeValueMillis(500), TimeValue.timeValueMillis(1), TimeValue.timeValueSeconds(300), Property.NodeScope);

    /**
     * 初始超时时间值。
     */
    private final TimeValue initialTimeout;
    /**
     * 退避时间值。
     */
    private final TimeValue backoffTime;
    /**
     * 最大超时时间值。
     */
    private final TimeValue maxTimeout;
    /**
     * 选举持续时间值。
     */
    private final TimeValue duration;
    /**
     * 线程池，用于执行异步任务。
     */
    private final ThreadPool threadPool;
    /**
     * 随机数生成器，用于生成随机选举时间。
     */
    private final Random random;
    public ElectionSchedulerFactory(Settings settings, Random random, ThreadPool threadPool) {
        this.random = random;
        this.threadPool = threadPool;

        initialTimeout = ELECTION_INITIAL_TIMEOUT_SETTING.get(settings);
        backoffTime = ELECTION_BACK_OFF_TIME_SETTING.get(settings);
        maxTimeout = ELECTION_MAX_TIMEOUT_SETTING.get(settings);
        duration = ELECTION_DURATION_SETTING.get(settings);

        if (maxTimeout.millis() < initialTimeout.millis()) {
            throw new IllegalArgumentException(new ParameterizedMessage("[{}] is [{}], but must be at least [{}] which is [{}]",
                ELECTION_MAX_TIMEOUT_SETTING_KEY, maxTimeout, ELECTION_INITIAL_TIMEOUT_SETTING_KEY, initialTimeout).getFormattedMessage());
        }
    }

    /**
     * 开始安排重复选举尝试的过程。
     *
     * @param gracePeriod       初始等待期，在此之后尝试第一次选举。
     * @param scheduledRunnable 每次选举尝试时应该执行的动作。
     * @return 返回一个可释放的调度器对象。
     */
    public Releasable startElectionScheduler(TimeValue gracePeriod, Runnable scheduledRunnable) {
        final ElectionScheduler scheduler = new ElectionScheduler(); // 创建选举调度器实例
        scheduler.scheduleNextElection(gracePeriod, scheduledRunnable); // 安排下一次选举
        return scheduler; // 返回调度器实例
    }

    /**
     * 确保数值为非负。
     * @param n 传入的长整型数值。
     * @return 返回非负的长整型数值。
     */
    @SuppressForbidden(reason = "Argument to Math.abs() is definitely not Long.MIN_VALUE") // 允许调用Math.abs()时传入Long.MIN_VALUE
    private static long nonNegative(long n) {
        return n == Long.MIN_VALUE ? 0 : Math.abs(n); // 如果是Long.MIN_VALUE，返回0；否则返回其绝对值
    }

    /**
     * 将随机数转换为正数，且不超过给定的上限。
     *
     * @param randomNumber 随机选择的长整型数值。
     * @param upperBound  包含的上限值。
     * @return 返回一个在(0, upperBound]范围内的正数。
     */
// package-private for testing 仅供包内测试使用
    static long toPositiveLongAtMost(long randomNumber, long upperBound) {
        assert 0 < upperBound : upperBound; // 断言检查upperBound大于0
        return nonNegative(randomNumber) % upperBound + 1; // 确保随机数非负，然后取模加1，以确保结果在指定范围内
    }

    /**
     * 返回ElectionSchedulerFactory对象的字符串表示。
     * @return 包含初始超时、退避时间和最大超时的字符串。
     */
    @Override
    public String toString() {
        return "ElectionSchedulerFactory{" +
            "initialTimeout=" + initialTimeout + // 初始超时时间
            ", backoffTime=" + backoffTime + // 退避时间
            ", maxTimeout=" + maxTimeout + // 最大超时时间
            '}';
    }
    private class ElectionScheduler implements Releasable {
        // 用于标记调度器是否已关闭
        private final AtomicBoolean isClosed = new AtomicBoolean();
        // 用于记录选举尝试的次数
        private final AtomicLong attempt = new AtomicLong();

        /**
         * 安排下一次选举。
         * @param gracePeriod 初始等待期，在此之后尝试第一次选举。
         * @param scheduledRunnable 每次选举尝试时应该执行的动作。
         */
        void scheduleNextElection(final TimeValue gracePeriod, final Runnable scheduledRunnable) {
            if (isClosed.get()) {
                // 如果调度器已关闭，则不安排选举
                logger.debug("{} not scheduling election", this);
                return;
            }

            // 获取当前尝试次数并递增
            final long thisAttempt = attempt.getAndIncrement();
            // 计算最大延迟时间，取初始超时时间和退避时间的最小值
            final long maxDelayMillis = Math.min(maxTimeout.millis(), initialTimeout.millis() + thisAttempt * backoffTime.millis());
            // 计算延迟时间，包括随机延迟和初始等待期
            final long delayMillis = toPositiveLongAtMost(random.nextLong(), maxDelayMillis) + gracePeriod.millis();
            // 创建执行选举的Runnable
            final Runnable runnable = new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    // 记录失败信息
                    logger.debug(new ParameterizedMessage("unexpected exception in wakeup of {}", this), e);
                    assert false : e;
                }

                @Override
                protected void doRun() {
                    if (isClosed.get()) {
                        // 如果调度器已关闭，则不开始选举
                        logger.debug("{} not starting election", this);
                    } else {
                        // 记录开始选举的信息，并递归安排下一次选举
                        logger.debug("{} starting election", this);
                        scheduleNextElection(duration, scheduledRunnable);
                        scheduledRunnable.run();
                    }
                }

                @Override
                public String toString() {
                    // 返回Runnable的字符串表示
                    return "scheduleNextElection{gracePeriod=" + gracePeriod
                        + ", thisAttempt=" + thisAttempt
                        + ", maxDelayMillis=" + maxDelayMillis
                        + ", delayMillis=" + delayMillis
                        + ", " + ElectionScheduler.this + "}";
                }
            };

            // 记录安排选举的信息
            logger.debug("scheduling {}", runnable);
            // 使用线程池安排执行
            threadPool.scheduleUnlessShuttingDown(TimeValue.timeValueMillis(delayMillis), Names.GENERIC, runnable);
        }

        @Override
        public String toString() {
            // 返回ElectionScheduler的字符串表示
            return "ElectionScheduler{attempt=" + attempt
                + ", " + ElectionSchedulerFactory.this + "}";
        }

        @Override
        public void close() {
            // 关闭调度器，确保不会重复关闭
            boolean wasNotPreviouslyClosed = isClosed.compareAndSet(false, true);
            assert wasNotPreviouslyClosed;
        }
    }
}
