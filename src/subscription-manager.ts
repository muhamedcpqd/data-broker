/* jslint node: true */
"use strict";

// import engine = require("./subscription-engine");

import {SubscriptionEngine, SubscriptionType} from "./subscription-engine";

import bodyParser = require("body-parser");
import express = require("express");
import http = require("http");
import morgan = require("morgan");
import util = require("util");
import { authEnforce, authParse, IAuthRequest} from "./api/authMiddleware";
import dojotLibs = require("dojot-libs");
import {SocketIOSingleton} from "./socketIo";
import { TopicManagerBuilder } from "./TopicBuilder";

// For now, express is not so well supported in TypeScript.
// A quick workaround, which apparently does not have any side effects is to
// set app with type "any".
// https://github.com/DefinitelyTyped/DefinitelyTyped/issues/21371#issuecomment-344958250
const app: any = express();
app.use(authParse);
app.use(authEnforce);
app.use(bodyParser.json()); // for parsing application/json
app.use(bodyParser.urlencoded({ extended: true })); // for parsing application/x-www-form-urlencoded
app.use(morgan("short"));

const engine = new SubscriptionEngine();

const httpServer = http.createServer(app);
// make sure singleton is instantiated
SocketIOSingleton.getInstance(httpServer);

/*
 *setting log debug route to app
 */
dojotLibs.loggerRoute(app, 'subscription-manager');

/*
 * Subscription management endpoints
 */
app.post("/subscription", (request: IAuthRequest, response: express.Response) => {
  const subscription = request.body;
  dojotLibs.logger.debug("Received new subscription request.", {filename: "subscription-manager"});
  dojotLibs.logger.debug(`Subscription body is: ${util.inspect(subscription, {depth: null})}`, 
  {filename: "subscription-manager"});
  if ("id" in subscription.subject.entities) {
    engine.addSubscription(SubscriptionType.id, subscription.subject.entities.id, subscription);
  } else if ("model" in subscription.subject.entities) {
    engine.addSubscription(SubscriptionType.model, subscription.subject.entities.model, subscription);
  } else if ("type" in subscription.subject.entities) {
    engine.addSubscription(SubscriptionType.type, subscription.subject.entities.type, subscription);
  }
  response.send("Ok!");
});

/*
 * Topic registry endpoints
 */
app.get("/topic/:subject", (req: IAuthRequest, response: express.Response) => {
  dojotLibs.logger.debug("Received a topic GET request.", {filename: "subscription-manager"});
  if (req.service === undefined) {
    dojotLibs.logger.error("Service is not defined in GET request headers.", {filename: "subscription-manager"});
    response.status(401);
    response.send({error: "missing mandatory authorization header in get request"});
  } else {
    const topics = TopicManagerBuilder.get(req.service);
    dojotLibs.logger.debug(`Topic for service ${req.service} and subject ${req.params.subject}.`, 
    {filename: "subscription-manager"});
    topics.getCreateTopic(req.params.subject, (error: any, data: any) => {
      if (error) {
        dojotLibs.logger.error(`Failed to retrieve topic. Error is ${error}`, {filename: "subscription-manager"});
        response.status(500);
        response.send({error: "failed to process topic"});
      } else {
        response.status(200).send({topic: data});
      }
    });
  }
});

/**
 * SocketIO endpoint
 */
app.get("/socketio", (req: IAuthRequest, response: express.Response) => {
  dojotLibs.logger.debug("Received a request for a new socketIO connection.", {filename: "subscription-manager"});
  if (req.service === undefined) {
    dojotLibs.logger.error("Service is not defined in SocketIO connection request headers.", 
    {filename: "subscription-manager"});
    response.status(401);
    response.send({ error: "missing mandatory authorization header in socketio request" });
  } else {
    const token = SocketIOSingleton.getInstance().getToken(req.service);
    response.status(200).send({ token });
  }
});

httpServer.listen(80, () => {
  dojotLibs.logger.debug("Subscription manager listening on port 80", {filename: "subscription-manager"});
});
