package org.apache.hadoop.test;

import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;

import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;

import java.util.EnumSet;

public class RMAppImplTest {


//    private static final StateMachineFactory<RMAppImpl,
//            RMAppState,
//            RMAppEventType,
//            RMAppEvent> stateMachineFactory
//            = new StateMachineFactory<RMAppImpl,
//            RMAppState,
//            RMAppEventType,
//            RMAppEvent>(RMAppState.NEW)
//
//
//            // Transitions from NEW state
//            .addTransition(RMAppState.NEW, RMAppState.NEW,
//                    RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
//            .addTransition(RMAppState.NEW, RMAppState.NEW_SAVING,
//                    RMAppEventType.START, new RMAppNewlySavingTransition())
//            .addTransition(RMAppState.NEW, EnumSet.of(RMAppState.SUBMITTED,
//                    RMAppState.ACCEPTED, RMAppState.FINISHED, RMAppState.FAILED,
//                    RMAppState.KILLED, RMAppState.FINAL_SAVING),
//                    RMAppEventType.RECOVER, new RMAppRecoveredTransition())
//            .addTransition(RMAppState.NEW, RMAppState.KILLED, RMAppEventType.KILL,
//                    new AppKilledTransition())
//            .addTransition(RMAppState.NEW, RMAppState.FINAL_SAVING,
//                    RMAppEventType.APP_REJECTED,
//                    new FinalSavingTransition(new AppRejectedTransition(),
//                            RMAppState.FAILED))
//
//            // Transitions from NEW_SAVING state
//            .addTransition(RMAppState.NEW_SAVING, RMAppState.NEW_SAVING,
//                    RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
//            .addTransition(RMAppState.NEW_SAVING, RMAppState.SUBMITTED,
//                    RMAppEventType.APP_NEW_SAVED, new AddApplicationToSchedulerTransition())
//            .addTransition(RMAppState.NEW_SAVING, RMAppState.FINAL_SAVING,
//                    RMAppEventType.KILL,
//                    new FinalSavingTransition(
//                            new AppKilledTransition(), RMAppState.KILLED))
//            .addTransition(RMAppState.NEW_SAVING, RMAppState.FINAL_SAVING,
//                    RMAppEventType.APP_REJECTED,
//                    new FinalSavingTransition(new AppRejectedTransition(),
//                            RMAppState.FAILED))
//            .addTransition(RMAppState.NEW_SAVING, RMAppState.FAILED,
//                    RMAppEventType.APP_SAVE_FAILED, new AppRejectedTransition())
//
//            // Transitions from SUBMITTED state
//            .addTransition(RMAppState.SUBMITTED, RMAppState.SUBMITTED,
//                    RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
//            .addTransition(RMAppState.SUBMITTED, RMAppState.FINAL_SAVING,
//                    RMAppEventType.APP_REJECTED,
//                    new FinalSavingTransition(
//                            new AppRejectedTransition(), RMAppState.FAILED))
//            .addTransition(RMAppState.SUBMITTED, RMAppState.ACCEPTED,
//                    RMAppEventType.APP_ACCEPTED, new StartAppAttemptTransition())
//            .addTransition(RMAppState.SUBMITTED, RMAppState.FINAL_SAVING,
//                    RMAppEventType.KILL,
//                    new FinalSavingTransition(
//                            new AppKilledTransition(), RMAppState.KILLED))
//
//            // Transitions from ACCEPTED state
//            .addTransition(RMAppState.ACCEPTED, RMAppState.ACCEPTED,
//                    RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
//            .addTransition(RMAppState.ACCEPTED, RMAppState.RUNNING,
//                    RMAppEventType.ATTEMPT_REGISTERED, new RMAppStateUpdateTransition(
//                            YarnApplicationState.RUNNING))
//            .addTransition(RMAppState.ACCEPTED,
//                    EnumSet.of(RMAppState.ACCEPTED, RMAppState.FINAL_SAVING),
//                    // ACCEPTED state is possible to receive ATTEMPT_FAILED/ATTEMPT_FINISHED
//                    // event because RMAppRecoveredTransition is returning ACCEPTED state
//                    // directly and waiting for the previous AM to exit.
//                    RMAppEventType.ATTEMPT_FAILED,
//                    new AttemptFailedTransition(RMAppState.ACCEPTED))
//            .addTransition(RMAppState.ACCEPTED, RMAppState.FINAL_SAVING,
//                    RMAppEventType.ATTEMPT_FINISHED,
//                    new FinalSavingTransition(FINISHED_TRANSITION, RMAppState.FINISHED))
//            .addTransition(RMAppState.ACCEPTED, RMAppState.KILLING,
//                    RMAppEventType.KILL, new KillAttemptTransition())
//            .addTransition(RMAppState.ACCEPTED, RMAppState.FINAL_SAVING,
//                    RMAppEventType.ATTEMPT_KILLED,
//                    new FinalSavingTransition(new AppKilledTransition(), RMAppState.KILLED))
//            .addTransition(RMAppState.ACCEPTED, RMAppState.ACCEPTED,
//                    RMAppEventType.APP_RUNNING_ON_NODE,
//                    new AppRunningOnNodeTransition())
//            // Handle AppAttemptLaunch to upate the launchTime and publish to ATS
//            .addTransition(RMAppState.ACCEPTED, RMAppState.ACCEPTED,
//                    RMAppEventType.ATTEMPT_LAUNCHED,
//                    new AttemptLaunchedTransition())
//
//            // Transitions from RUNNING state
//            .addTransition(RMAppState.RUNNING, RMAppState.RUNNING,
//                    RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
//            .addTransition(RMAppState.RUNNING, RMAppState.FINAL_SAVING,
//                    RMAppEventType.ATTEMPT_UNREGISTERED,
//                    new FinalSavingTransition(
//                            new AttemptUnregisteredTransition(),
//                            RMAppState.FINISHING, RMAppState.FINISHED))
//            .addTransition(RMAppState.RUNNING, RMAppState.FINISHED,
//                    // UnManagedAM directly jumps to finished
//                    RMAppEventType.ATTEMPT_FINISHED, FINISHED_TRANSITION)
//            .addTransition(RMAppState.RUNNING, RMAppState.RUNNING,
//                    RMAppEventType.APP_RUNNING_ON_NODE,
//                    new AppRunningOnNodeTransition())
//            .addTransition(RMAppState.RUNNING,
//                    EnumSet.of(RMAppState.ACCEPTED, RMAppState.FINAL_SAVING),
//                    RMAppEventType.ATTEMPT_FAILED,
//                    new AttemptFailedTransition(RMAppState.ACCEPTED))
//            .addTransition(RMAppState.RUNNING, RMAppState.KILLING,
//                    RMAppEventType.KILL, new KillAttemptTransition())
//
//            // Transitions from FINAL_SAVING state
//            .addTransition(RMAppState.FINAL_SAVING,
//                    EnumSet.of(RMAppState.FINISHING, RMAppState.FAILED,
//                            RMAppState.KILLED, RMAppState.FINISHED), RMAppEventType.APP_UPDATE_SAVED,
//                    new FinalStateSavedTransition())
//            .addTransition(RMAppState.FINAL_SAVING, RMAppState.FINAL_SAVING,
//                    RMAppEventType.ATTEMPT_FINISHED,
//                    new AttemptFinishedAtFinalSavingTransition())
//            .addTransition(RMAppState.FINAL_SAVING, RMAppState.FINAL_SAVING,
//                    RMAppEventType.APP_RUNNING_ON_NODE,
//                    new AppRunningOnNodeTransition())
//            // ignorable transitions
//            .addTransition(RMAppState.FINAL_SAVING, RMAppState.FINAL_SAVING,
//                    EnumSet.of(RMAppEventType.NODE_UPDATE, RMAppEventType.KILL,
//                            RMAppEventType.APP_NEW_SAVED))
//
//            // Transitions from FINISHING state
//            .addTransition(RMAppState.FINISHING, RMAppState.FINISHED,
//                    RMAppEventType.ATTEMPT_FINISHED, FINISHED_TRANSITION)
//            .addTransition(RMAppState.FINISHING, RMAppState.FINISHING,
//                    RMAppEventType.APP_RUNNING_ON_NODE,
//                    new AppRunningOnNodeTransition())
//            // ignorable transitions
//            .addTransition(RMAppState.FINISHING, RMAppState.FINISHING,
//                    EnumSet.of(RMAppEventType.NODE_UPDATE,
//                            // ignore Kill/Move as we have already saved the final Finished state
//                            // in state store.
//                            RMAppEventType.KILL))
//
//            // Transitions from KILLING state
//            .addTransition(RMAppState.KILLING, RMAppState.KILLING,
//                    RMAppEventType.APP_RUNNING_ON_NODE,
//                    new AppRunningOnNodeTransition())
//            .addTransition(RMAppState.KILLING, RMAppState.FINAL_SAVING,
//                    RMAppEventType.ATTEMPT_KILLED,
//                    new FinalSavingTransition(
//                            new AppKilledTransition(), RMAppState.KILLED))
//            .addTransition(RMAppState.KILLING, RMAppState.FINAL_SAVING,
//                    RMAppEventType.ATTEMPT_UNREGISTERED,
//                    new FinalSavingTransition(
//                            new AttemptUnregisteredTransition(),
//                            RMAppState.FINISHING, RMAppState.FINISHED))
//            .addTransition(RMAppState.KILLING, RMAppState.FINISHED,
//                    // UnManagedAM directly jumps to finished
//                    RMAppEventType.ATTEMPT_FINISHED, FINISHED_TRANSITION)
//            .addTransition(RMAppState.KILLING,
//                    EnumSet.of(RMAppState.FINAL_SAVING),
//                    RMAppEventType.ATTEMPT_FAILED,
//                    new AttemptFailedTransition(RMAppState.KILLING))
//
//            .addTransition(RMAppState.KILLING, RMAppState.KILLING,
//                    EnumSet.of(
//                            RMAppEventType.NODE_UPDATE,
//                            RMAppEventType.ATTEMPT_REGISTERED,
//                            RMAppEventType.APP_UPDATE_SAVED,
//                            RMAppEventType.KILL))
//
//            // Transitions from FINISHED state
//            // ignorable transitions
//            .addTransition(RMAppState.FINISHED, RMAppState.FINISHED,
//                    RMAppEventType.APP_RUNNING_ON_NODE,
//                    new AppRunningOnNodeTransition())
//            .addTransition(RMAppState.FINISHED, RMAppState.FINISHED,
//                    EnumSet.of(
//                            RMAppEventType.NODE_UPDATE,
//                            RMAppEventType.ATTEMPT_UNREGISTERED,
//                            RMAppEventType.ATTEMPT_FINISHED,
//                            RMAppEventType.KILL))
//
//            // Transitions from FAILED state
//            // ignorable transitions
//            .addTransition(RMAppState.FAILED, RMAppState.FAILED,
//                    RMAppEventType.APP_RUNNING_ON_NODE,
//                    new AppRunningOnNodeTransition())
//            .addTransition(RMAppState.FAILED, RMAppState.FAILED,
//                    EnumSet.of(RMAppEventType.KILL, RMAppEventType.NODE_UPDATE))
//
//            // Transitions from KILLED state
//            // ignorable transitions
//            .addTransition(RMAppState.KILLED, RMAppState.KILLED,
//                    RMAppEventType.APP_RUNNING_ON_NODE,
//                    new AppRunningOnNodeTransition())
//            .addTransition(
//                    RMAppState.KILLED,
//                    RMAppState.KILLED,
//                    EnumSet.of(RMAppEventType.APP_ACCEPTED,
//                            RMAppEventType.APP_REJECTED, RMAppEventType.KILL,
//                            RMAppEventType.ATTEMPT_FINISHED, RMAppEventType.ATTEMPT_FAILED,
//                            RMAppEventType.NODE_UPDATE, RMAppEventType.START))
//
//            .installTopology();


    public static void main(String[] args) {
//        StateMachine stateMachine = stateMachineFactory.make(this);
//        System.out.println(stateMachine);

//        RMAppAttempt attempt =
//                new RMAppAttemptImpl(null, null, null, null,
//                        null, null, null, null, null);
    }


}
