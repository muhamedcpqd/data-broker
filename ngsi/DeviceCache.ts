/* jslint node: true */
"use strict";

import axios, {AxiosError, AxiosResponse} from "axios";
import util = require("util");
import dojotLibs = require('dojot-libs');
import * as device from "./deviceManager";

type DeviceRequestCallback = (err: any, data: device.IDevice | undefined) => void;

function generateJWT(service: string) {
  return (new Buffer("dummy jwt schema").toString("base64")) + "."
         + (new Buffer(JSON.stringify({service})).toString("base64")) + "."
         + (new Buffer("dummy signature").toString("base64"));
}

class DeviceCache {
  private deviceInfo: {[id: string]: device.IDevice};
  private deviceManager: string;

  constructor(deviceManager: string) {
    this.deviceInfo = {};
    this.deviceManager = deviceManager;
  }

  public getDeviceInfo(service: string, deviceId: string, cb: DeviceRequestCallback): void {
    dojotLibs.logger.debug("Got a request for device info...", {filename: "device-cache"});
    const data = this.deviceInfo[this.parseKey(service, deviceId)];
    if (data === undefined) {
      dojotLibs.logger.debug("No data in cache.", {filename: "device-cache"});
      this.requestDeviceInfo(service, deviceId, cb);
    } else {
      cb(null, data);
    }
  }

  private requestDeviceInfo(service: string, deviceId: string, cb: DeviceRequestCallback): void {
    dojotLibs.logger.debug("Retrieving device info from DeviceManager...", {filename: "device-cache"});
    axios({
      headers: {authorization: "Bearer " + generateJWT(service)},
      method: "get",
      url: this.deviceManager + "/device/" + deviceId,
    })
    .then((response: AxiosResponse) => {
      dojotLibs.logger.debug("... device info was retrieved.", {filename: "device-cache"});
      dojotLibs.logger.debug(`Data is: ${util.inspect(response.data, {depth: null})}`, {filename: "device-cache"});
      this.deviceInfo[this.parseKey(service, deviceId)] = response.data;
      cb(null, response.data);
    })
    .catch((error: AxiosError) => {
      dojotLibs.logger.debug("... device info was not retrieved.", {filename: "device-cache"});
      dojotLibs.logger.error(`Error is: ${error}`);
      cb(error, undefined);
    });
    dojotLibs.logger.debug("... request for DeviceManager was sent.", {filename: "device-cache"});
  }

  private parseKey(service: string, deviceId: string): string {
    return service + "::" + deviceId;
  }
}

export { DeviceCache };
