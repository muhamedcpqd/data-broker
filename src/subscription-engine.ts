/* jslint node: true */
"use strict";

import kafka = require("kafka-node");
import util = require("util");
import { KafkaConsumer } from "./consumer";
import dojotLibs = require("dojot-libs");
import { KafkaProducer } from "./producer";
import tools = require("./simple-tools");

import { Event } from "./subscription/Event";
import { Notification } from "./subscription/Notification";
import { Subscription } from "./subscription/Subscription";

interface IAction {
  topic: string;
  data: any;
}

// Now this is awful, but
enum SubscriptionType {
  model = "model",
  type = "type",
  id = "id",
  flat = "flat",
}

interface ISubscriptionMap {
  [key: string]: any;
}

interface IRegisteredSubscriptions {
  // This will allow "invalid" types on indirect access to attributes, but we need it for
  // the compiler to allow indirect (e.g. var[valuePointer] ) attribute reading/writing
  [k: string]: ISubscriptionMap;
  flat: ISubscriptionMap;
  id: ISubscriptionMap;
  model: ISubscriptionMap;
  type: ISubscriptionMap;
}

const operators = ["==", "!=", ">=", "<=", "~=", ">", "<" ];

function evaluateLogicTest(op1: any, operator: string, op2: string): boolean {
  dojotLibs.logger.debug(`Evaluating logic test: ${op1} ${operator} ${op2}.`, 
  {filename: "subscription-engine"});
  // There"s something here
  switch (operator) {
    case "==":
      return (op1 === op2);
    case "!=":
      return (op1 !== op2);
    case ">":
      return (op1 > parseFloat(op2));
    case ">=":
      return (op1 >= parseFloat(op2));
    case "<":
      return (op1 < parseFloat(op2));
    case "<=":
      return (op1 <= parseFloat(op2));
    case "~=":
      // ret = (logicTokens[1].exec(data[logicTokens[0]].value).length != 0);
  }
  return false;
}

function evaluateLogicCondition(condition: string, data: any) {
  dojotLibs.logger.debug("Evaluating logic condition...", 
  {filename: "subscription-engine"});
  let ret = true;

  const logicTests = tools.tokenize(condition, ";");

  for (const logicTest of logicTests) {
    for (const operator of operators) {
      const logicTokens = tools.tokenize(logicTest, operator);
      if (logicTokens.length <= 1) {
        continue;
      }
      ret = evaluateLogicTest(data[logicTokens[0]].value, operator, logicTokens[1]);
      dojotLibs.logger.debug(`Condition evaluation result so far: ${ret}.`, 
      {filename: "subscription-engine"});
      if (ret === false) {
        break;
      }
    }
    if (ret === false) {
      break;
    }
  }

  dojotLibs.logger.debug("... logic condition was evaluated.", 
  {filename: "subscription-engine"});
  return ret;
}

function evaluateMetaCondition(condition: string, data: any) {
  const ret = true;
  dojotLibs.logger.debug("Evaluation of meta-data is not yet implemented.", 
  {filename: "subscription-engine"});

  dojotLibs.logger.debug("Parameters are:", {filename: "subscription-engine"});
  dojotLibs.logger.debug(`Condition: ${condition}`, {filename: "subscription-engine"});
  dojotLibs.logger.debug(`Data: ${util.inspect(data, { depth: null})}`, 
  {filename: "subscription-engine"});

  return ret;
}

function evaluateGeoCondition(georel: string, geometry: string, coords: string, data: any) {
  const ret = true;
  dojotLibs.logger.debug("Evaluation of meta-data is not yet implemented.", 
  {filename: "subscription-engine"});
  dojotLibs.logger.debug("Parameters are:", {filename: "subscription-engine"});
  dojotLibs.logger.debug(`georel: ${georel}`, {filename: "subscription-engine"});
  dojotLibs.logger.debug(`geometry: ${geometry}`, {filename: "subscription-engine"});
  dojotLibs.logger.debug(`coords: ${coords}`, {filename: "subscription-engine"});
  dojotLibs.logger.debug(`Data: ${util.inspect(data, { depth: null})}`, 
  {filename: "subscription-engine"});
  return ret;
}

function evaluateCondition(condition: any, data: any) {
  let ret = true;

  if ("q" in condition) {
    ret = evaluateLogicCondition(condition.q, data);
  }

  if ("mq" in condition) {
    ret = ret && evaluateMetaCondition(condition.mq, data);
  }

  if ("georel" in condition) {
    ret = ret && evaluateGeoCondition(condition.georel, condition.geometry, condition.coords, data);
  }

  return ret;
}

function generateOutputData(obj: Event, notification: Notification): IAction {
  const ret: IAction = {
    data: {},
    topic: notification.topic,
  };

  // notification.attrs contains all the attributes that must be
  // forwarded to output.
  // obj.attrs contains all the data retrieved from the device
  for (const attr of notification.attrs) {
    if (attr in obj.attrs) {
      ret.data[attr] = obj.attrs[attr].value;
    }
  }

  return ret;
}

function checkSubscriptions(obj: Event, subscriptions: Subscription[]): IAction[] {
  const actions: IAction[] = [];

  for (const subscription of subscriptions) {
    if (subscription.subject.condition != null) {
      if (subscription.subject.condition!.attrs != null) {
        // This subscription has some associated attributes, let"s check them
        const subscAttrs = subscription.subject.condition!.attrs;
        for (let j = 0; j < subscAttrs.length; j++) {
          if (subscription.subject.condition!.attrs[j] in obj.attrs) {
            // This subscription should be evaluated;
            if (subscription.subject.condition!.expression != null) {
              // TODO Gather all data from the device - the condition might use a few
              // variables that were not registered with this subscription
              if (evaluateCondition(subscription.subject.condition!.expression, obj.attrs)) {
                dojotLibs.logger.debug(`I should send something to ${subscription.notification.topic}`, 
                {filename: "subscription-engine"});
                
                actions.push(generateOutputData(obj, subscription.notification));
              }
            } else {
              // There is no condition to this subscription - it should be triggered
              dojotLibs.logger.debug(`I should send something to ${subscription.notification.topic}`, 
              {filename: "subscription-engine"});
              
              actions.push(generateOutputData(obj, subscription.notification));
            }
            break;
          }
        }
      } else {
        // All attributes should evaluate this subscription
        dojotLibs.logger.debug(`I should send something to ${subscription.notification.topic}`, 
        {filename: "subscription-engine"});
        
        actions.push(generateOutputData(obj, subscription.notification));
      }
    } else {
      // This subscription will be triggered whenever a message is sent by this device
      dojotLibs.logger.debug(`I should send something to ${subscription.notification.topic}`, 
      {filename: "subscription-engine"});
      
      actions.push(generateOutputData(obj, subscription.notification));
    }
  }

  return actions;
}

class SubscriptionEngine {
  private producer: KafkaProducer;
  private producerReady: boolean;

  private subscriber: KafkaConsumer;

  private registeredSubscriptions: IRegisteredSubscriptions;

  constructor() {
    dojotLibs.logger.debug("Initializing subscription engine...", 
    {filename: "subscription-engine"});

    this.producerReady = false;
    this.producer = new KafkaProducer(undefined, () => {
      this.producerReady = true;
    });

    this.subscriber = new KafkaConsumer();

    this.registeredSubscriptions = {
      flat: {},
      id: {},
      model: {},
      type: {},
    };
  }

  public handleEvent(err: any, message: kafka.Message | undefined) {
    if (err) {
      dojotLibs.logger.error(`Subscriber reported error: ${err}`, 
      {filename: "subscription-engine"});
      return;
    }

    if (message === undefined) {
      dojotLibs.logger.error("Received an empty message.", 
      {filename: "subscription-engine"});
      return;
    }

    if (this.producerReady === false) {
      dojotLibs.logger.error("Got event before being ready to handle it, ignoring", 
      {filename: "subscription-engine"});
      return;
    }

    let data: string;
    dojotLibs.logger.debug("New data arrived!", {filename: "subscription-engine"});
    try {
      data = JSON.parse(message.value);
      dojotLibs.logger.debug(`Data: ${util.inspect(data, {depth: null})}`, 
      {filename: "subscription-engine"});
      
      this.processEvent(new Event(data));
    } catch (err) {
      if (err instanceof TypeError) {
        dojotLibs.logger.error(`Received data is not a valid event: ${message.value}`,
        {filename: "subscription-engine"});

      } else if (err instanceof SyntaxError) {
        dojotLibs.logger.error(`Failed to parse event as JSON: ${message.value}`, 
        {filename: "subscription-engine"});
      }
    }
  }

  public addIngestionChannel(topic: string[]) {
    const kafkaTopics: kafka.Topic[] = [];
    for (const i in topic) {
      if (topic.hasOwnProperty(i)) {
        kafkaTopics.push({topic: i});
      }
    }
    this.subscriber.subscribe(kafkaTopics, this.handleEvent);
  }

  public addSubscription(type: SubscriptionType, key: string, subscription: Subscription) {
    this.registeredSubscriptions.flat[subscription.id] = subscription;
    if (!(key in this.registeredSubscriptions[type])) {
      this.registeredSubscriptions[type][key] = [];
    }
    this.registeredSubscriptions[type][key].push(subscription);
    if (subscription.notification != null) {
      this.producer.createTopics([subscription.notification.topic]);
    }
  }

  private processEvent(obj: Event) {
    let subscriptions;
    let actions: IAction[] = [];

    // Check whether there"s any subscriptions to this device id
    if (obj.metadata.deviceid in this.registeredSubscriptions.id) {
      // There are subscriptions for this device ID
      subscriptions = this.registeredSubscriptions.id[obj.metadata.deviceid];
      actions = actions.concat(checkSubscriptions(obj, subscriptions));
    }

    // Check whether there"s any subscriptions to this model
    if (obj.metadata.model in this.registeredSubscriptions.model) {
      // There are subscriptions for this device ID
      subscriptions = this.registeredSubscriptions.model[obj.metadata.model];
      actions = actions.concat(checkSubscriptions(obj, subscriptions));
    }

    // Check whether there"s any subscriptions to this device type
    if (obj.metadata.type in this.registeredSubscriptions.type) {
      // There are subscriptions for this device ID
      subscriptions = this.registeredSubscriptions.type[obj.metadata.type];
      actions = actions.concat(checkSubscriptions(obj, subscriptions));
    }

    // Execute all actions
    for (const action of actions) {
      this.producer.send(JSON.stringify(action.data), action.topic);
    }
  }
}

export { SubscriptionType, SubscriptionEngine };
