/* jslint node: true */
"use strict";

import uuid = require("uuid/v4");
import {broker as config} from "./config";
import dojotLibs = require("dojot-libs");
import { KafkaProducer } from "./producer";
import { QueuedTopic } from "./QueuedTopic";
import { ClientWrapper } from "./RedisClientWrapper";
import { RedisManager } from "./redisManager";

type TopicCallback = (error?: any, topic?: string) => void;

// TODO this should also handle kafka ACL configuration
class TopicManager {
  private redis: ClientWrapper;
  private service: string;
  private getSet: string;
  private producer: KafkaProducer;
  private producerReady: boolean;
  private topicQueue: QueuedTopic[];

  constructor(service: string) {
    if ((service === undefined) || service.length === 0) {
      throw new Error("a valid service id must be supplied");
    }

    this.service = service;
    this.redis = RedisManager.getClient();
    this.getSet = __dirname + "/lua/setGet.lua";
    this.producerReady = false;
    this.topicQueue = [];
    this.producer = new KafkaProducer(undefined, () => {
      this.producerReady = true;
      if (this.topicQueue.length) {
        for (const request of this.topicQueue) {
          this.handleRequest(request);
        }
      }
    });
  }

  public getCreateTopic(subject: string, callback: TopicCallback | undefined): void {
    dojotLibs.logger.debug("Retrieving/creating new topic...", {filename: "topicManager"});
    dojotLibs.logger.debug(`Subject: ${subject}`, {filename: "topicManager"});
    try {
      const key: string = this.parseKey(subject);
      const tid: string = uuid();
      this.redis.runScript(this.getSet, [key], [tid], (err: any, topic: string) => {
        if (err && callback) {
          dojotLibs.logger.debug("... topic could not be created/retrieved.", {filename: "topicManager"});
          dojotLibs.logger.error(`Error while calling REDIS: ${err}`, {filename: "topicManager"});
          callback(err);
        }

        dojotLibs.logger.debug("... topic was properly created/retrievied.", {filename: "topicManager"});
        const request = {topic, subject, callback};
        if (this.producerReady) {
          dojotLibs.logger.debug("Handling all pending requests...", {filename: "topicManager"});
          this.handleRequest(request);
          dojotLibs.logger.debug("... all pending requests were handled.", {filename: "topicManager"});
        } else {
          dojotLibs.logger.debug("Producer is not yet ready.", {filename: "topicManager"});
          dojotLibs.logger.debug("Adding to the pending requests queue...", {filename: "topicManager"});
          this.topicQueue.push(request);
          dojotLibs.logger.debug("... topic was added to queue.", {filename: "topicManager"});
        }
      });
    } catch (error) {
      dojotLibs.logger.debug("... topic could not be created/retrieved.", {filename: "topicManager"});
      dojotLibs.logger.error(`An exception was thrown: ${error}`, {filename: "topicManager"});
      if (callback) {
        callback(error);
      }
    }
  }

  public destroy() {
    dojotLibs.logger.debug("Closing down this topic manager...", {filename: "topicManager"});
    this.producer.close();
    dojotLibs.logger.debug("... topic manager was closed.", {filename: "topicManager"});
  }

  private assertTopic(topicid: string, message: string): void {
    if ((topicid === undefined) || topicid.length === 0) {
      throw new Error(message);
    }
  }

  private parseKey(subject: string) {
    this.assertTopic(subject, "a valid subject must be provided");
    return "ti:" + this.service + ":" + subject;
  }

  private handleRequest(request: QueuedTopic) {
    this.producer.createTopics([request.topic], () => {
      if (config.ingestion.find((i) => request.subject === i)) {
        // Subject is used for data ingestion - initialize consumer
        dojotLibs.logger.debug(`Will initialize ingestion handler ${request.subject} at topic ${request.topic}.`, 
        {filename: "topicManager"});
      }

      if (request.callback) {
        request.callback(undefined, request.topic);
      }
    });
  }
}

export { TopicCallback, TopicManager };
