/* jslint node: true */
"use strict";

import express = require("express");
import dojotLibs = require("dojot-libs");
import { InvalidTokenError } from "./InvalidTokenError";
import { UnauthorizedError } from "./UnauthorizedError";

/**
 * Open up a base64 encoded string.
 * @param data The data to be decoded.
 */
function b64decode(data: string): string {
  if (typeof Buffer.from === "function") {
    return Buffer.from(data, "base64").toString();
  } else {
    return (new Buffer(data, "base64")).toString();
  }
}

/**
 * Interface for handling authorization requests.
 * All parameters should be optional, as pointed out here:
 * https://stackoverflow.com/questions/49303375/property-push-is-missing-in-type-request-ntlmrequest-response-response
 */
interface IAuthRequest extends express.Request {
  user?: string;
  userid?: string;
  service?: string;
}

function authParse(req: IAuthRequest, res: express.Response, next: express.NextFunction) {
  const rawToken = req.header("authorization");
  if (rawToken === undefined) {
    next();
    return;
  }

  const token = rawToken!.split(".");
  if (token.length !== 3) {
    dojotLibs.logger.error("Got invalid request: token is malformed.", {filename: "auth"});
    dojotLibs.logger.error(`Token is: ${rawToken}`, {filename: "auth"});
    res.status(401).send(new InvalidTokenError());
    return;
  }

  const tokenData = JSON.parse(b64decode(token[1]));

  req.user = tokenData.username;
  req.userid = tokenData.userid;
  req.service = tokenData.service;
  next();
}

function authEnforce(req: IAuthRequest, res: express.Response, next: express.NextFunction) {
  if (req.user === undefined || req.user!.trim() === "" ) {
    // valid token must be supplied
    dojotLibs.logger.error("Got invalid request: user is not defined in token.", {filename: "auth"});
    dojotLibs.logger.error(`Token is: ${req.header("authorization")}`, {filename: "auth"});
    res.status(401).send(new UnauthorizedError());
  } else if (req.service === undefined || req.service!.trim() === "" ) {
    // valid token must be supplied
    dojotLibs.logger.error("Got invalid request: service is not defined in token.", {filename: "auth"});
    dojotLibs.logger.error(`Token is: ${req.header("authorization")}`, {filename: "auth"});
    res.status(401).send(new UnauthorizedError());
  } else {
    next();
  }
}

export { IAuthRequest, authParse, authEnforce };
