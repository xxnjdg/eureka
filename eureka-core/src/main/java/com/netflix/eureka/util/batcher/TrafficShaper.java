/*
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.eureka.util.batcher;

import com.netflix.eureka.util.batcher.TaskProcessor.ProcessingResult;

/**
 * {@link TrafficShaper} provides admission control policy prior to dispatching tasks to workers.
 * It reacts to events coming via reprocess requests (transient failures, congestion), and delays the processing
 * depending on this feedback.
 *
 * @author Tomasz Bak
 */
class TrafficShaper {

    /**
     * Upper bound on delay provided by configuration.
     */
    private static final long MAX_DELAY = 30 * 1000;

    private final long congestionRetryDelayMs;
    private final long networkFailureRetryMs;

    private volatile long lastCongestionError;
    private volatile long lastNetworkFailure;

    TrafficShaper(long congestionRetryDelayMs, long networkFailureRetryMs) {
        this.congestionRetryDelayMs = Math.min(MAX_DELAY, congestionRetryDelayMs);
        this.networkFailureRetryMs = Math.min(MAX_DELAY, networkFailureRetryMs);
    }

    void registerFailure(ProcessingResult processingResult) {
        if (processingResult == ProcessingResult.Congestion) {
            // 最后一次阻塞导致提交批处理失败的时间
            lastCongestionError = System.currentTimeMillis();
        } else if (processingResult == ProcessingResult.TransientError) {
            // 最后一次网络原因导致提交批处理失败的时间
            lastNetworkFailure = System.currentTimeMillis();
        }
    }

    // 计算传输延迟的时间
    long transmissionDelay() {
        if (lastCongestionError == -1 && lastNetworkFailure == -1) {
            return 0;
        }

        long now = System.currentTimeMillis();
        if (lastCongestionError != -1) {
            // 阻塞延迟时间
            long congestionDelay = now - lastCongestionError;
            if (congestionDelay >= 0 && congestionDelay < congestionRetryDelayMs) {
                return congestionRetryDelayMs - congestionDelay;
            }
            lastCongestionError = -1;
        }

        if (lastNetworkFailure != -1) {
            // 网络延迟时间
            long failureDelay = now - lastNetworkFailure;
            if (failureDelay >= 0 && failureDelay < networkFailureRetryMs) {
                return networkFailureRetryMs - failureDelay;
            }
            lastNetworkFailure = -1;
        }
        return 0;
    }
}
