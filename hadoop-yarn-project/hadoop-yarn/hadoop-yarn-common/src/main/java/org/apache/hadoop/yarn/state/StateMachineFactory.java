/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.yarn.state;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * 状态机拓扑。
 * 该对象在语义上是不可变的。
 * 如果您拥有 StateMachineFactory ，则API中没有可更改其语义属性的操作。
 * @param <OPERAND> 状态机操作此对象类型.
 * @param <STATE> 实体的状态.
 * @param <EVENTTYPE> 需要处理的外部事件.
 * @param <EVENT> 事件对象.
 *
 * State machine topology.
 * This object is semantically immutable.  If you have a
 * StateMachineFactory there's no operation in the API that changes
 * its semantic properties.
 *
 * @param <OPERAND> The object type on which this state machine operates.
 * @param <STATE> The state of the entity.
 * @param <EVENTTYPE> The external eventType to be handled.
 * @param <EVENT> The event object.
 *
 *
 *
 *
 *
 *
 */
@Public
@Evolving
final public class StateMachineFactory  <OPERAND, STATE extends Enum<STATE>,  EVENTTYPE extends Enum<EVENTTYPE>, EVENT> {


  // transitionsListNode的主要作用的把状态机中的transition按照状态转移的顺利逆序的链成一个链表。
  private final TransitionsListNode transitionsListNode;


  // 状态机表
  // [状态 -> <事件类型->操作>  ]

  // <OPERAND> 状态机操作此对象类型.
  // <STATE> 实体的状态.
  // <EVENTTYPE> 需要处理的外部事件.
  // <EVENT> 事件对象.

  private Map<STATE, Map<EVENTTYPE,  Transition<OPERAND, STATE, EVENTTYPE, EVENT>>> stateMachineTable;

  // 默认初始化状态
  private STATE defaultInitialState;

  // 是否优化, 用于构建 makeStateMachineTable
  private final boolean optimized;

  /**
   * Constructor
   *
   * This is the only constructor in the API.
   *
   */
  public StateMachineFactory(STATE defaultInitialState) {
    this.transitionsListNode = null;
    this.defaultInitialState = defaultInitialState;
    this.optimized = false;
    this.stateMachineTable = null;
  }
  // 私有构造方法 : no.1
  // addTransition内部创建StateMachineFactory对象。
  private StateMachineFactory(StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> that,
       ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT> t) {
    this.defaultInitialState = that.defaultInitialState;
    this.transitionsListNode 
        = new TransitionsListNode(t, that.transitionsListNode);
    this.optimized = false;
    this.stateMachineTable = null;
  }

  // 私有构造方法 : no.2
  // 这个比较特殊: StateMachineFactory.installTopology() 方法调用  ==>  optimized : true
  // addTransition内部创建StateMachineFactory对象。
  private StateMachineFactory(StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> that,  boolean optimized) {
    this.defaultInitialState = that.defaultInitialState;
    this.transitionsListNode = that.transitionsListNode;
    this.optimized = optimized;

    // 当optimized为true是，
    // 会在构造函数中调用makeStateMachineTable对stateMachineTable进行赋值。
    if (optimized) {
      makeStateMachineTable();
    } else {
      stateMachineTable = null;
    }
  }

  // ApplicableTransition是一个接口，有一个apply方法
  private interface ApplicableTransition
             <OPERAND, STATE extends Enum<STATE>,
              EVENTTYPE extends Enum<EVENTTYPE>, EVENT> {
    void apply(StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> subject);
  }


  // 链表的数据结构  本次转换和下一个结点。
  private class TransitionsListNode {


    // ApplicableSingleOrMultipleTransition 实现了该接口
    final ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT> transition;

    final TransitionsListNode next;

    // 从构造函数中可以看出transition是当前状态转移对应的处理类，
    // next指向的是下一个TransitionsListNode，
    TransitionsListNode
        (ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT> transition,
        TransitionsListNode next) {

      this.transition = transition;
      // 此时的下一个TransitionsListNode其实是上一个StateMachineFactory中的TransitionListNode。
      this.next = next;
    }
  }
  // addTransition内StateMachineFactory的参数ApplicableSingleOrMultipleTransition

  // 主要是将preState、eventType和transition的映射关系放入stateMachineTable属性中。
  static private class ApplicableSingleOrMultipleTransition
             <OPERAND, STATE extends Enum<STATE>,
              EVENTTYPE extends Enum<EVENTTYPE>, EVENT>
          implements ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT> {
    final STATE preState;
    final EVENTTYPE eventType;
    final Transition<OPERAND, STATE, EVENTTYPE, EVENT> transition;

    ApplicableSingleOrMultipleTransition
        (STATE preState, EVENTTYPE eventType,
         Transition<OPERAND, STATE, EVENTTYPE, EVENT> transition) {
      this.preState = preState;
      this.eventType = eventType;
      this.transition = transition;
    }

    @Override
    public void apply  (StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> subject) {

      // 从stateMachineTable中拿到preState对应的transitionMap
      Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>> transitionMap = subject.stateMachineTable.get(preState);

      // transitionMap为null则new一个transitionMap的HashMap
      if (transitionMap == null) {
        // I use HashMap here because I would expect most EVENTTYPE's to not
        //  apply out of a particular state, so FSM sizes would be 
        //  quadratic if I use EnumMap's here as I do at the top level.
        transitionMap = new HashMap<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>>();
        // 更新前置节点的 preState, transitionMap
        subject.stateMachineTable.put(preState, transitionMap);
      }
      // 将eventType对应的Transition放入transitionMap中
      transitionMap.put(eventType, transition);
    }
  }

  /**
   *
   * 一个初始状态、一个最终状态、一种事件
   *
   * @return a NEW StateMachineFactory just like {@code this} with the current
   *          transition added as a new legal transition.  This overload
   *          has no hook object.
   *
   *         Note that the returned StateMachineFactory is a distinct
   *         object.
   *
   *         This method is part of the API.
   *
   * @param preState pre-transition state
   * @param postState post-transition state
   * @param eventType stimulus for the transition
   */
  // 构造方法 no.1
  // 一个初始状态、一个最终状态、一种事件
  public StateMachineFactory
             <OPERAND, STATE, EVENTTYPE, EVENT>
          addTransition(STATE preState, STATE postState, EVENTTYPE eventType) {
    return addTransition(preState, postState, eventType, null);
  }


  /**
   * @return a NEW StateMachineFactory just like {@code this} with the current
   *          transition added as a new legal transition
   *
   *         Note that the returned StateMachineFactory is a distinct object.
   *
   *         This method is part of the API.
   *
   * @param preState pre-transition state
   * @param postState post-transition state
   * @param eventType stimulus for the transition
   * @param hook transition hook
   */
  // 构造方法 no.1
  // [  1-1-1-1  引用 + 1 ] 一个初始状态、一个最终状态，一个事件，一个回调;
  public StateMachineFactory
          <OPERAND, STATE, EVENTTYPE, EVENT>
  addTransition(STATE preState, STATE postState,
                EVENTTYPE eventType,
                SingleArcTransition<OPERAND, EVENT> hook){
    return new StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT>(
            this,
            new ApplicableSingleOrMultipleTransition<OPERAND, STATE, EVENTTYPE, EVENT> (preState, eventType, new SingleInternalArc(postState, hook)));
  }

  /**
   * @return a NEW StateMachineFactory just like {@code this} with the current
   *          transition added as a new legal transition.  This overload
   *          has no hook object.
   *
   *
   *         Note that the returned StateMachineFactory is a distinct
   *         object.
   *
   *         This method is part of the API.
   *
   * @param preState pre-transition state
   * @param postState post-transition state
   * @param eventTypes List of stimuli for the transitions
   */
  // 一个初始状态、多种事件、一个最终状态、无回调函数
  public StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> addTransition(
      STATE preState, STATE postState, Set<EVENTTYPE> eventTypes) {
    return addTransition(preState, postState, eventTypes, null);
  }

  /**
   * @return a NEW StateMachineFactory just like {@code this} with the current
   *          transition added as a new legal transition
   *
   *         Note that the returned StateMachineFactory is a distinct
   *         object.
   *
   *         This method is part of the API.
   *
   * @param preState pre-transition state
   * @param postState post-transition state
   * @param eventTypes List of stimuli for the transitions
   * @param hook transition hook
   */
  // 一个初始状态、多种事件、一个最终状态 、 一个回调函数
  // [  1-*-1-1  引用 + 1 ]
  public StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> addTransition(
      STATE preState, STATE postState, Set<EVENTTYPE> eventTypes,
      SingleArcTransition<OPERAND, EVENT> hook) {

    StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> factory = null;

    for (EVENTTYPE event : eventTypes) {
      if (factory == null) {
        // 构造方法 no.1   : 构建StateMachineFactory 头信息
        factory = addTransition(preState, postState, event, hook);
      } else {
        // 构造方法 no.1
        factory = factory.addTransition(preState, postState, event, hook);
      }
    }
    return factory;
  }

  /**
   * @return a NEW StateMachineFactory just like {@code this} with the current
   *          transition added as a new legal transition
   *
   *         Note that the returned StateMachineFactory is a distinct object.
   *
   *         This method is part of the API.
   *
   * @param preState pre-transition state
   * @param postStates valid post-transition states
   * @param eventType stimulus for the transition
   * @param hook transition hook
   */
  // 第三种 : 一个初始状态、多个最终状态，一个事件，多个回调
  public StateMachineFactory  <OPERAND, STATE, EVENTTYPE, EVENT>  addTransition(STATE preState,
                        Set<STATE> postStates,
                        EVENTTYPE eventType,
                        MultipleArcTransition<OPERAND, EVENT, STATE> hook){
    return new StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT>
        (this,
         new ApplicableSingleOrMultipleTransition<OPERAND, STATE, EVENTTYPE, EVENT>
           (preState, eventType, new MultipleInternalArc(postStates, hook)));
  }

  /**
   * @return a StateMachineFactory just like {@code this}, except that if
   *         you won't need any synchronization to build a state machine
   *
   *         Note that the returned StateMachineFactory is a distinct object.
   *
   *         This method is part of the API.
   *
   *         The only way you could distinguish the returned
   *         StateMachineFactory from {@code this} would be by
   *         measuring the performance of the derived 
   *         {@code StateMachine} you can get from it.
   *
   * Calling this is optional.  It doesn't change the semantics of the factory,
   *   if you call it then when you use the factory there is no synchronization.
   *
   * 调用installTopology进行状态链的初始化。
   *
   */
  public StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> installTopology() {
    // 实例化了一个新的StateMachineFactory，不同的是将属性optimized设置为true。
    // 当optimized为true是，会在构造函数中调用makeStateMachineTable对stateMachineTable进行赋值。
    return new StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT>(this, true);
  }

  /**
   * Effect a transition due to the effecting stimulus.
   * @param oldState current state
   * @param eventType trigger to initiate the transition
   * @param event causal eventType context
   * @return transitioned state
   */
  private STATE doTransition (OPERAND operand, STATE oldState, EVENTTYPE eventType, EVENT event)
      throws InvalidStateTransitionException {
    // We can assume that stateMachineTable is non-null because we call
    //  maybeMakeStateMachineTable() when we build an InnerStateMachine ,
    //  and this code only gets called from inside a working InnerStateMachine .
    Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>> transitionMap = stateMachineTable.get(oldState);
    if (transitionMap != null) {
      Transition<OPERAND, STATE, EVENTTYPE, EVENT> transition = transitionMap.get(eventType);
      if (transition != null) {
        return transition.doTransition(operand, oldState, event, eventType);
      }
    }
    throw new InvalidStateTransitionException(oldState, eventType);
  }

  private synchronized void maybeMakeStateMachineTable() {
    if (stateMachineTable == null) {
      makeStateMachineTable();
    }
  }

  private void makeStateMachineTable() {
    // 声明一个stack数据结构，用来存储TransitionsListNode链中的TransitionsListNode
    // 之所以用stack，是因为TransitionsListNode链是按照状态转移的逆序排列的
    // 也就是说TransitionsListNode链中的第一个是状态转移中最后一个状态对应的ApplicableSingleOrMultipleTransition
    Stack<ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT>> stack = new Stack<ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT>>();

    Map<STATE, Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>>>
      prototype = new HashMap<STATE, Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>>>();

    prototype.put(defaultInitialState, null);

    // I use EnumMap here because it'll be faster and denser.  I would
    //  expect most of the states to have at least one transition.
    stateMachineTable
       = new EnumMap<STATE, Map<EVENTTYPE,
                           Transition<OPERAND, STATE, EVENTTYPE, EVENT>>>(prototype);


    // 将TransitionsListNode链中的ApplicableSingleOrMultipleTransition入stack
    for (TransitionsListNode cursor = transitionsListNode;
         cursor != null;
         cursor = cursor.next) {
      stack.push(cursor.transition);
    }

    // 将ApplicableSingleOrMultipleTransition出stack，
    // 调用apply对stateMachineTable进行赋值
    while (!stack.isEmpty()) {
      stack.pop().apply(this);
    }
  }

  // SingleInternalArc 和 MultipleInternalArc 实现Transition的接口
  // Transition接口中定义了doTransition来定义State的转换
  private interface Transition<OPERAND, STATE extends Enum<STATE>,
          EVENTTYPE extends Enum<EVENTTYPE>, EVENT> {
    STATE doTransition(OPERAND operand, STATE oldState,
                       EVENT event, EVENTTYPE eventType);
  }


  // SingleInteralArc 实现了Transition，
  // 用来处理一个状态被一个事件触发之后转移到下一个状态
  //
  // 有转换之后的状态postState和SingleArcTransition类型的转换，
  // 在SingleInternalArc中的doTransition方法返回了转换之后的状态。
  private class SingleInternalArc
                    implements Transition<OPERAND, STATE, EVENTTYPE, EVENT> {

    //
    private STATE postState;

    // 表示事件发生之后调用对应hook进行处理的SingleArcTransition属性。
    private SingleArcTransition<OPERAND, EVENT> hook; // transition hook

    SingleInternalArc(STATE postState,
        SingleArcTransition<OPERAND, EVENT> hook) {
      this.postState = postState;
      this.hook = hook;
    }

    // 调用hook.transition去处理发生该事件之后的状态变化，
    // hook正常处理结束之后，返回postState状态。
    @Override
    public STATE doTransition(OPERAND operand, STATE oldState,
                              EVENT event, EVENTTYPE eventType) {
      if (hook != null) {
        hook.transition(operand, event);
      }
      return postState;
    }
  }

  // MultipleInternalArc
  private class MultipleInternalArc  implements Transition<OPERAND, STATE, EVENTTYPE, EVENT>{

    // Fields
    // 存储可供选择的postState状态
    private Set<STATE> validPostStates;

    // // 事件对应的hook
    private MultipleArcTransition<OPERAND, EVENT, STATE> hook;  // transition hook

    MultipleInternalArc(Set<STATE> postStates,
                   MultipleArcTransition<OPERAND, EVENT, STATE> hook) {
      this.validPostStates = postStates;
      this.hook = hook;
    }

    @Override
    public STATE doTransition(OPERAND operand, STATE oldState,
                              EVENT event, EVENTTYPE eventType)
        throws InvalidStateTransitionException {


      STATE postState = hook.transition(operand, event);

      // 校验postState是否在可选的状态集中
      if (!validPostStates.contains(postState)) {
        throw new InvalidStateTransitionException(oldState, eventType);
      }
      return postState;
    }
  }

  /**
   * A StateMachine that accepts a transition listener.
   * @param operand the object upon which the returned
   *                {@link StateMachine} will operate.
   * @param initialState the state in which the returned
   *                {@link StateMachine} will start.
   * @param listener An implementation of a {@link StateTransitionListener}.
   * @return A (@link StateMachine}.
   */
  public StateMachine<STATE, EVENTTYPE, EVENT>
        make(OPERAND operand, STATE initialState,
             StateTransitionListener<OPERAND, EVENT, STATE> listener) {
    return new InternalStateMachine(operand, initialState, listener);
  }

  /* 
   * @return a {@link StateMachine} that starts in 
   *         {@code initialState} and whose {@link Transition} s are
   *         applied to {@code operand} .
   *
   *         This is part of the API.
   *
   * @param operand the object upon which the returned 
   *                {@link StateMachine} will operate.
   * @param initialState the state in which the returned 
   *                {@link StateMachine} will start.
   *                
   */
  public StateMachine<STATE, EVENTTYPE, EVENT>
        make(OPERAND operand, STATE initialState) {
    return new InternalStateMachine(operand, initialState);
  }

  /* 
   * @return a {@link StateMachine} that starts in the default initial
   *          state and whose {@link Transition} s are applied to
   *          {@code operand} . 
   *
   *         This is part of the API.
   *
   * @param operand the object upon which the returned 
   *                {@link StateMachine} will operate.
   *                
   */
  public StateMachine<STATE, EVENTTYPE, EVENT> make(OPERAND operand) {
    return new InternalStateMachine(operand, defaultInitialState);
  }

  private static class NoopStateTransitionListener implements StateTransitionListener {
    @Override
    public void preTransition(Object op, Enum beforeState,  Object eventToBeProcessed) { }

    @Override
    public void postTransition(Object op, Enum beforeState, Enum afterState,  Object processedEvent) { }
  }

  //
  private static final NoopStateTransitionListener NOOP_LISTENER =  new NoopStateTransitionListener();

  // 状态机的具体实现类
  private class InternalStateMachine implements StateMachine<STATE, EVENTTYPE, EVENT> {
    private final OPERAND operand;
    private STATE currentState;
    private final StateTransitionListener<OPERAND, EVENT, STATE> listener;

    InternalStateMachine(OPERAND operand, STATE initialState) {
      this(operand, initialState, null);
    }

    InternalStateMachine(OPERAND operand, STATE initialState,
        StateTransitionListener<OPERAND, EVENT, STATE> transitionListener) {
      this.operand = operand;
      this.currentState = initialState;
      this.listener =
          (transitionListener == null) ? NOOP_LISTENER : transitionListener;
      if (!optimized) {
        maybeMakeStateMachineTable();
      }
    }

    @Override
    public synchronized STATE getCurrentState() {
      return currentState;
    }

    @Override
    public synchronized STATE doTransition(EVENTTYPE eventType, EVENT event)
         throws InvalidStateTransitionException  {


      listener.preTransition(operand, currentState, event);

      STATE oldState = currentState;

      // 从stateMachineTable中得到当前state和eventType对应的transition，
      // 然后调用transition.doTransition方法，
      // 调用eventType对应的hook去执行相关逻辑。
      currentState = StateMachineFactory.this.doTransition
          (operand, currentState, eventType, event);
      listener.postTransition(operand, oldState, currentState, event);
      return currentState;
    }
  }

  /**
   * Generate a graph represents the state graph of this StateMachine
   * @param name graph name
   * @return Graph object generated
   */
  @SuppressWarnings("rawtypes")
  public Graph generateStateGraph(String name) {
    maybeMakeStateMachineTable();
    Graph g = new Graph(name);
    for (STATE startState : stateMachineTable.keySet()) {
      Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>> transitions
          = stateMachineTable.get(startState);
      for (Entry<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>> entry :
         transitions.entrySet()) {
        Transition<OPERAND, STATE, EVENTTYPE, EVENT> transition = entry.getValue();
        if (transition instanceof StateMachineFactory.SingleInternalArc) {
          StateMachineFactory.SingleInternalArc sa
              = (StateMachineFactory.SingleInternalArc) transition;
          Graph.Node fromNode = g.getNode(startState.toString());
          Graph.Node toNode = g.getNode(sa.postState.toString());
          fromNode.addEdge(toNode, entry.getKey().toString());
        } else if (transition instanceof StateMachineFactory.MultipleInternalArc) {
          StateMachineFactory.MultipleInternalArc ma
              = (StateMachineFactory.MultipleInternalArc) transition;
          Iterator iter = ma.validPostStates.iterator();
          while (iter.hasNext()) {
            Graph.Node fromNode = g.getNode(startState.toString());
            Graph.Node toNode = g.getNode(iter.next().toString());
            fromNode.addEdge(toNode, entry.getKey().toString());
          }
        }
      }
    }
    return g;
  }
}
