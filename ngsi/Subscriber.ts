/* jslint node: true */
"use strict";

import {ArgumentParser} from "argparse";
import axios from "axios";
import kafka = require("kafka-node");
import uuid = require("uuid/v4");
import dojotLibs = require("dojot-libs");
import { DeviceCache } from "./DeviceCache";
import * as device from "./deviceManager";
import { TranslatorV1 } from "./TranslatorV1";
import { TranslatorV2 } from "./TranslatorV2";

const parser = new ArgumentParser();
parser.addArgument(["-t", "--topic"], {required: true});
parser.addArgument(["-k", "--kafka"], {required: true});
parser.addArgument(["-o", "--target"], {required: true});
parser.addArgument(["--deviceManager"], {defaultValue: "http://device-manager:5000"});
parser.addArgument(["--group"], {defaultValue: uuid()});
parser.addArgument(["--version"], {defaultValue: "v1"});
const args = parser.parseArgs();

const cache = new DeviceCache(args.deviceManager);

interface ITranslator {
  /**
   * Translates to NGSI format
   * @param  {any}            deviceData Current values to be embedded in the event
   * @param  {device.Device}  deviceInfo Device information model, as configured in device-manager
   * @param  {string}         topic      Topic in which the event has been received
   * @return {any}                       Object containing event in NGSI format
   */
  translate(deviceData: any, deviceInfo: device.IDevice, topic: string): any;
}

let translator: ITranslator;
if (args.version === "v1") {
  translator = new TranslatorV1();
} else if (args.version === "v2") {
  translator = new TranslatorV2();
} else {
  dojotLibs.logger.error("Unknown version " + args.version + " requested.", {filename: "device-cache"});
  process.exit(1);
}

function handleMessage(data: kafka.Message) {

  const event = JSON.parse(data.value);
  const meta = event.metadata;
  cache.getDeviceInfo(meta.service, meta.deviceid, (err: any, deviceInfo: device.IDevice | undefined) => {
    if (err || (deviceInfo === undefined)) {
      dojotLibs.logger.error("Failed to process received event", err, {filename: "device-cache"});
      return;
    }

    const translated = translator.translate(event.attrs, deviceInfo, data.topic);
    if (translated == null) {
      dojotLibs.logger.error("Failed to parse event", event, {filename: "device-cache"});
    }

    axios({
      data: translated,
      headers: {"content-type": "application/json"},
      method: "post",
      url: args.target,
    })
    .then(() => { dojotLibs.logger.debug("event sent"); })
    .catch(() => { dojotLibs.logger.debug("failed to send request"); });
  });

}

const options = { kafkaHost: args.kafka, groupId: args.group};
const consumer = new kafka.ConsumerGroup(options, args.topic);
consumer.on("message", handleMessage);
consumer.on("error", (err) => { dojotLibs.logger.error("kafka consumer error", err, {filename: "device-cache"}); });
