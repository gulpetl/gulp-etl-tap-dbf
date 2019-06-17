"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const through2 = require('through2');
const PluginError = require("plugin-error");
const pkginfo = require('pkginfo')(module); // project package.json info into module.exports
const PLUGIN_NAME = module.exports.name;
const loglevel = require("loglevel");
//import { read, createReadStream } from 'fs';
const log = loglevel.getLogger(PLUGIN_NAME); // get a logger instance based on the project name
//log.setLevel((process.env.DEBUG_LEVEL || 'warn') as log.LogLevelDesc)
const toStream = require('buffer-to-stream');
const getStream = require('get-stream');
const YADBF = require('yadbf');
/** wrap incoming recordObject in a Singer RECORD Message object*/
function createRecord(recordObject, streamName) {
    return { type: "RECORD", stream: streamName, record: recordObject };
}
/* This is a gulp-etl plugin. It is compliant with best practices for Gulp plugins (see
https://github.com/gulpjs/gulp/blob/master/docs/writing-a-plugin/guidelines.md#what-does-a-good-plugin-look-like ),
and like all gulp-etl plugins it accepts a configObj as its first parameter */
function tapDbf(configObj) {
    // creating a stream through which each file will pass - a new instance will be created and invoked for each file 
    // see https://stackoverflow.com/a/52432089/5578474 for a note on the "this" param
    const strm = through2.obj(function (file, encoding, cb) {
        const self = this;
        let returnErr = null;
        // post-process line object
        const handleLine = (lineObj, _streamName) => {
            let newObj = createRecord(lineObj, _streamName);
            lineObj = newObj;
            return lineObj;
        };
        function newTransformer(streamName) {
            let transformer = through2.obj(); // new transform stream, in object mode
            // transformer is designed to follow yadbf, which emits objects, so dataObj is an Object. We will finish by converting dataObj to a text line
            transformer._transform = function (dataObj, encoding, callback) {
                let returnErr = null;
                try {
                    let handledObj = handleLine(dataObj, streamName);
                    if (handledObj) {
                        let handledLine = JSON.stringify(handledObj);
                        log.debug(handledLine);
                        this.push(handledLine + '\n');
                    }
                }
                catch (err) {
                    returnErr = new PluginError(PLUGIN_NAME, err);
                }
                callback(returnErr);
            };
            return transformer;
        }
        // set the stream name to the file name (without extension)
        let streamName = file.stem;
        if (file.isNull()) {
            // return empty file
            return cb(returnErr, file);
        }
        else if (file.isBuffer()) {
            const readable = toStream(Buffer.from(file.contents));
            let mystream = readable.pipe(new YADBF(configObj))
                .pipe(newTransformer(streamName))
                .on('finish', () => __awaiter(this, void 0, void 0, function* () {
                try {
                    file.contents = yield getStream.buffer(mystream);
                }
                catch (err) {
                    console.error(err);
                }
                cb(returnErr, file);
            }))
                .on('error', function (err) {
                log.error(err);
                self.emit('error', new PluginError(PLUGIN_NAME, err));
            });
        }
        else if (file.isStream()) {
            file.contents = file.contents
                .pipe(new YADBF(configObj))
                .on('end', function () {
                // DON'T CALL THIS HERE. It MAY work, if the job is small enough. But it needs to be called after the stream is SET UP, not when the streaming is DONE.
                // Calling the callback here instead of below may result in data hanging in the stream--not sure of the technical term, but dest() creates no file, or the file is blank
                // cb(returnErr, file);
                // log.debug('calling callback')    
                log.debug('dbf parser is done');
            })
                // .on('data', function (data:any, err: any) {
                //   log.debug(data)
                // })
                .on('error', function (err) {
                log.error(err);
                self.emit('error', new PluginError(PLUGIN_NAME, err));
            })
                .pipe(newTransformer(streamName));
            // after our stream is set up (not necesarily finished) we call the callback
            log.debug('calling callback');
            cb(returnErr, file);
        }
    });
    return strm;
}
exports.tapDbf = tapDbf;
//# sourceMappingURL=plugin.js.map