/* jslint node: true */
"use strict";

import kafka = require("kafka-node");
import sio = require("socket.io");
import uuid = require("uuid/v4");
import {KafkaConsumer} from "./consumer";
import dojotLibs = require("dojot-libs");
import {RedisManager} from "./redisManager";
import { TopicManagerBuilder } from "./TopicBuilder";

function getKey(token: string): string {
  return "si:" + token;
}

/**
 * Class used to handle SocketIO operations
 */
class SocketIOHandler {
  private ioServer: SocketIO.Server;
  private consumers: { [key: string]: KafkaConsumer };

  /**
   * Constructor.
   * @param httpServer HTTP server as a basis to offer SocketIO connection
   */
  constructor(httpServer: any) {
    dojotLibs.logger.debug("Creating new SocketIO handler...", {filename: "SocketIOHandler"});

    this.consumers = {};

    dojotLibs.logger.debug("Creating sio server...", {filename: "SocketIOHandler"});
    this.ioServer = sio(httpServer);
    dojotLibs.logger.debug("... sio server was created.", {filename: "SocketIOHandler"});

    this.ioServer.use(this.checkSocket);

    dojotLibs.logger.debug("Registering SocketIO server callbacks...", {filename: "SocketIOHandler"});
    this.ioServer.on("connection", (socket) => {
      dojotLibs.logger.debug("Got new SocketIO connection.", {filename: "SocketIOHandler"});
      const redis = RedisManager.getClient();
      const givenToken = socket.handshake.query.token;

      dojotLibs.logger.debug(`Received token is ${givenToken}.`, {filename: "SocketIOHandler"});

      redis.runScript(
        __dirname + "/lua/setDel.lua",
        [getKey(givenToken)],
        [],
        (error: any, tenant) => {
          if (error || !tenant) {
            dojotLibs.logger.error(
              `Failed to find suitable context for socket: ${socket.id}.`, {filename: "SocketIOHandler"});
            dojotLibs.logger.error("Disconnecting socket.", {filename: "SocketIOHandler"});
            socket.disconnect();
            return;
          }

          dojotLibs.logger.debug(
            `Will assign client [${givenToken}] to namespace: (${tenant}): ${
              socket.id
            }`, {filename: "SocketIOHandler"});
          socket.join(tenant);
        });
    });
    dojotLibs.logger.debug("... SocketIO server callbacks were registered.", {filename: "SocketIOHandler"});
    dojotLibs.logger.debug("... SocketIO handler was created.", {filename: "SocketIOHandler"});
  }

  /**
   * Generate a new token to be used in SocketIO connection.
   * @param tenant The tenant related to this new token
   */
  public getToken(tenant: string): string {
    dojotLibs.logger.debug(`Generating new token for tenant ${tenant}...`, {filename: "SocketIOHandler"});

    dojotLibs.logger.debug("Creating new topic/retrieving current topic in Kafka for this tenant...", 
    {filename: "SocketIOHandler"});
    const topicManager = TopicManagerBuilder.get(tenant);
    topicManager.getCreateTopic(
      "device-data",
      (error?: any, topic?: string) => {
        if (error || !topic) {
          dojotLibs.logger.error(
            `Failed to find appropriate topic for tenant: ${
              error ? error : "Unknown topic"
            }`, {filename: "SocketIOHandler"});
          return;
        }
        this.subscribeTopic(topic, tenant);
      });
    dojotLibs.logger.debug("... Kafka topic creation/retrieval was requested.", {filename: "SocketIOHandler"});

    dojotLibs.logger.debug("Associating tenant and SocketIO token...", {filename: "SocketIOHandler"});
    const token = uuid();
    const redis = RedisManager.getClient();
    redis.client.setex(getKey(token), 60, tenant);
    dojotLibs.logger.debug("... token and tenant were associated.", {filename: "SocketIOHandler"});

    dojotLibs.logger.debug(`... token for tenant ${tenant} was created: ${token}.`, {filename: "SocketIOHandler"});
    return token;
  }

  /**
   * Callback function used to process messages received from Kafka library.
   * @param nsp SocketIO namespace to send out messages to all subscribers. These are tenants.
   * @param error Error received from Kafka library.
   * @param message The message received from Kafka Library
   */
  private handleMessage(nsp: string, error?: any, message?: kafka.Message) {
    dojotLibs.logger.debug("Processing message just received...", {filename: "SocketIOHandler"});
    if (error || message === undefined) {
      dojotLibs.logger.error("Invalid event received. Ignoring.", {filename: "SocketIOHandler"});
      dojotLibs.logger.error(`Error is ${error}`, {filename: "SocketIOHandler"});
      dojotLibs.logger.error(`Message is ${message}`, {filename: "SocketIOHandler"});
      return;
    }

    let data: any;
    dojotLibs.logger.debug("Trying to parse received message payload...", {filename: "SocketIOHandler"});
    try {
      data = JSON.parse(message.value);
    } catch (err) {
      if (err instanceof TypeError) {
        dojotLibs.logger.debug("... message payload was not successfully parsed.", {filename: "SocketIOHandler"});
        dojotLibs.logger.error(`Received data is not a valid event: ${message.value}`, 
        {filename: "SocketIOHandler"});
      } else if (err instanceof SyntaxError) {
        dojotLibs.logger.debug("... message payload was not successfully parsed.", {filename: "SocketIOHandler"});
        dojotLibs.logger.error(`Failed to parse event as JSON: ${message.value}`, {filename: "SocketIOHandler"});
      }
      return;
    }
    dojotLibs.logger.debug("... message payload was successfully parsed.", {filename: "SocketIOHandler"});

    if (data.hasOwnProperty("metadata")) {
      if (!data.metadata.hasOwnProperty("deviceid")) {
        dojotLibs.logger.debug("... received message was not successfully processed.", {filename: "SocketIOHandler"});
        dojotLibs.logger.error(
          "Received data is not a valid dojot event - has no deviceid", {filename: "SocketIOHandler"});
        return;
      }
    } else {
      dojotLibs.logger.debug("... received message was not successfully processed.", {filename: "SocketIOHandler"});
      dojotLibs.logger.error(
        "Received data is not a valid dojot event - has no metadata", {filename: "SocketIOHandler"}, {filename: "SocketIOHandler"});
      return;
    }

    dojotLibs.logger.debug(`Will publish event to namespace ${nsp}: ${message.value}`, {filename: "SocketIOHandler"});
    this.ioServer.to(nsp).emit(data.metadata.deviceid, data);
    this.ioServer.to(nsp).emit("all", data);
    dojotLibs.logger.debug("... received message was successfully processed.", {filename: "SocketIOHandler"});
  }

  /**
   * Subscribe to a particular topic in Kafka.
   * @param topic The topic to be subscribed
   * @param tenant The tenant related to the topic being subscribed.
   * @returns A new KafkaConsumer
   */
  private subscribeTopic(topic: string, tenant: string): KafkaConsumer {
    dojotLibs.logger.debug(`Subscribing to topic ${topic}...`, {filename: "SocketIOHandler"});
    if (this.consumers.hasOwnProperty(topic)) {
      dojotLibs.logger.debug("Topic already had a subscription. Returning it.", {filename: "SocketIOHandler"});
      return this.consumers[topic];
    }

    dojotLibs.logger.debug(`Will subscribe to topic ${topic}`, {filename: "SocketIOHandler"});
    const subscriber = new KafkaConsumer();
    this.consumers[topic] = subscriber;
    subscriber.subscribe(
      [{ topic }],
      (error?: any, message?: kafka.Message) => {
        this.handleMessage(tenant, error, message);
      });

    dojotLibs.logger.debug("... topic was successfully subscribed.", {filename: "SocketIOHandler"});
    return subscriber;
  }

  /**
   *
   * @param socket The socket to be checked
   * @param next Next verification callback. Used by SocketIO library.
   */
  private checkSocket(socket: SocketIO.Socket, next: (error?: Error) => void) {
    const givenToken = socket.handshake.query.token;
    if (givenToken) {
      const redis = RedisManager.getClient();
      redis.client.get(getKey(givenToken), (error, value) => {
        if (error) {
          return next(new Error("Failed to verify token"));
        }
        if (value) {
          return next();
        } else {
          return next(new Error("Authentication error: unknown token"));
        }
      });
    } else {
      return next(new Error("Authentication error: missing token"));
    }
  }
}

export { SocketIOHandler };
