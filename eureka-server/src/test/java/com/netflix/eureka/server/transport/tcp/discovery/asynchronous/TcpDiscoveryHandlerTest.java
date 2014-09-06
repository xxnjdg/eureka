/*
 * Copyright 2014 Netflix, Inc.
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

package com.netflix.eureka.server.transport.tcp.discovery.asynchronous;

/**
 * @author Tomasz Bak
 */
public class TcpDiscoveryHandlerTest {
/*

    private final TestableEurekaService eurekaService = new TestableEurekaService();

    private final TestableObservableConnection<Object, Object> observableConnection = new TestableObservableConnection<Object, Object>();

    private final TcpDiscoveryHandler discoveryHandler = new TcpDiscoveryHandler(eurekaService);

    private final Observable<Void> handleObservable = discoveryHandler.handle(observableConnection);

    private final RecordingSubscriber<Void> handlerStatus = RecordingSubscriber.subscribeTo(handleObservable);

    @Test(timeout = 10000)
    public void testInterestRegistrationAndUnregistration() throws Exception {
        // Register
        TestableInterestChannel firstChannel = doInterestRegistration();

        // Unregister
        observableConnection.testableChannelRead().onNext(new UnregisterInterestSet());
        assertTrue("Channel shall be closed by now", RxBlocking.isCompleted(1, TimeUnit.SECONDS, firstChannel.viewClose()));

        // Register again
        TestableInterestChannel secondChannel = doInterestRegistration();
        assertNotEquals("Expected different channel", firstChannel, secondChannel);
    }

    @Test(timeout = 10000)
    public void testClientDisconnect() throws Exception {
        // Register
        TestableInterestChannel interestChannel = doInterestRegistration();

        // Simulate client disconnect.
        handlerStatus.getSubscription().unsubscribe();
        assertTrue("Channel should be closed by now", RxBlocking.isCompleted(1, TimeUnit.SECONDS, interestChannel.viewClose()));
    }

    @Test(timeout = 10000)
    public void testAddInstanceNotification() throws Exception {
        TestableInterestChannel interestChannel = doInterestRegistration();
        sendInterestUpdate(interestChannel, new ChangeNotification<InstanceInfo>(Kind.Add, SampleInstanceInfo.DiscoveryServer.build()));
    }

    @Test(timeout = 10000)
    public void testDeleteInstanceNotification() throws Exception {
        TestableInterestChannel interestChannel = doInterestRegistration();
        sendInterestUpdate(interestChannel, new ChangeNotification<InstanceInfo>(Kind.Delete, SampleInstanceInfo.DiscoveryServer.build()));
    }

    @Test(timeout = 10000)
    public void testModifyInstanceNotification() throws Exception {
        TestableInterestChannel interestChannel = doInterestRegistration();

        List<Delta> deltas = new ArrayList<Delta>(1);
        deltas.add(SampleDelta.StatusUp.build());

        sendInterestUpdate(interestChannel, new ModifyNotification<InstanceInfo>(SampleInstanceInfo.DiscoveryServer.build(), deltas));
    }

    private void sendInterestUpdate(TestableInterestChannel interestChannel, ChangeNotification<InstanceInfo> notification) {
        Iterator updatesIterator = RxBlocking.iteratorFrom(1, TimeUnit.SECONDS, observableConnection.testableChannelWrite());
        interestChannel.submitNotification(notification);

        InterestSetNotification expectedMessage = protocolMessageFrom(notification);
        assertEquals("Unexpected or missing protocol message", expectedMessage, updatesIterator.next());
    }

    private InterestSetNotification protocolMessageFrom(ChangeNotification<InstanceInfo> notification) {
        switch (notification.getKind()) {
            case Add:
                return new AddInstance(notification.getData());
            case Delete:
                return new DeleteInstance(notification.getData().getId());
            case Modify:
                return new UpdateInstanceInfo(((ModifyNotification<InstanceInfo>) notification).getDelta().iterator().next());
        }
        return null;
    }

    private TestableInterestChannel doInterestRegistration() throws InterruptedException {
        observableConnection.testableChannelRead().onNext(new InterestRegistration(SampleInterest.DiscoveryApp.build()));
        TestableInterestChannel interestChannel = (TestableInterestChannel) eurekaService.viewNewInterestChannels().poll(1, TimeUnit.SECONDS);
        assertTrue("Active connection expected", !handlerStatus.isDone());
        assertNotNull("Expected registered channel instance", interestChannel);
        return interestChannel;
    }
*/
}