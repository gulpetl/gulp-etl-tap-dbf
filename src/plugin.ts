const through2 = require('through2')
import Vinyl = require('vinyl')
import PluginError = require('plugin-error');
const pkginfo = require('pkginfo')(module); // project package.json info into module.exports
const PLUGIN_NAME = module.exports.name;
import * as loglevel from 'loglevel'
//import { read, createReadStream } from 'fs';
const log = loglevel.getLogger(PLUGIN_NAME) // get a logger instance based on the project name
log.setLevel((process.env.DEBUG_LEVEL || 'warn') as log.LogLevelDesc)
const toStream = require('buffer-to-stream');
const getStream = require('get-stream');
const YADBF = require('yadbf')

/** wrap incoming recordObject in a Singer RECORD Message object*/
function createRecord(recordObject:Object, streamName: string) : any {
  return {type:"RECORD", stream:streamName, record:recordObject}
}


/* This is a gulp-etl plugin. It is compliant with best practices for Gulp plugins (see
https://github.com/gulpjs/gulp/blob/master/docs/writing-a-plugin/guidelines.md#what-does-a-good-plugin-look-like ),
and like all gulp-etl plugins it accepts a configObj as its first parameter */
export function tapDbf(configObj: any) {
 
let count = 0
  // creating a stream through which each file will pass - a new instance will be created and invoked for each file 
  // see https://stackoverflow.com/a/52432089/5578474 for a note on the "this" param
  const strm = through2.obj(function (this: any, file: Vinyl, encoding: string, cb: Function) {
    const self = this
    let returnErr: any = null
   

    // post-process line object
    const handleLine = (lineObj: any, _streamName : string): object | null => {
      try {  
        let newObj = createRecord(lineObj, _streamName)
          lineObj = newObj
      }
      catch (err) {
        log.error(err)
      }
      return lineObj
    }

    
    function newTransformer(streamName : string) {

      let transformer = through2.obj(); // new transform stream, in object mode
  
      // transformer is designed to follow yadbf, which emits objects, so dataObj is an Object. We will finish by converting dataObj to a text line
      transformer._transform = function (dataObj: Object, encoding: string, callback: Function) {
        count++

callback(null, JSON.stringify(dataObj))
log.debug(count + ".")
return
        let returnErr: any = null
        try {
          let handledObj = handleLine(dataObj, streamName)
          if (handledObj) {
            let handledLine = JSON.stringify(handledObj)
            log.debug(count + ". data from handleLine:" + handledLine)
            this.push(handledLine + '\n')
          }
        } catch (err) {
          returnErr = new PluginError(PLUGIN_NAME, err);
        } 

        callback(returnErr)
      }
      return transformer
    }
    
    // set the stream name to the file name (without extension)
    let streamName : string = file.stem

    if (file.isNull()) {
      // return empty file
      return cb(returnErr, file)
    }

    else if (file.isBuffer()) {

      let dataCount = 0
      
            try {
      
            const readable = toStream(file.contents)
            let mystream = readable.pipe(new YADBF(configObj))
            .on('error', function (err: any) {
              log.error(err)
              self.emit('error', new PluginError(PLUGIN_NAME, err));
            })
            .on('data', (record:any) => {
              dataCount++
              log.debug('data from YADBF: ' + dataCount + ': ' + JSON.stringify(record))
            })
            .on('end', () => {
              console.log('Done!');
            })      
            .pipe(newTransformer(streamName))
            // .on('error', function (err: any) {
            //   log.error(err)
            //   self.emit('error', new PluginError(PLUGIN_NAME, err));
            // })
            .on('finish',async () => {
              try {
                  let zzzz =  await getStream.buffer(mystream)
                  file.contents = zzzz
                  log.debug('finished')
                  // let asdf =  getStream.buffer(mystream)
                  // asdf
                  // .then((asdf:any) => {
      
                  //   file.contents = asdf
                    cb(returnErr, file);
                  // })
                  // .catch((err:any) => {
                  //   console.error(err)
                  // })
              }
              catch (err) {
               console.error(err)
              }
            })
          }
            catch (err) {
              log.error(err)
            }
      
          }
    
    else if (file.isStream()) {
      file.contents = file.contents
        .pipe(new YADBF(configObj))
 
        .on('end', function () {

          // DON'T CALL THIS HERE. It MAY work, if the job is small enough. But it needs to be called after the stream is SET UP, not when the streaming is DONE.
          // Calling the callback here instead of below may result in data hanging in the stream--not sure of the technical term, but dest() creates no file, or the file is blank
          // cb(returnErr, file);
          // log.debug('calling callback')    

          log.debug('dbf parser is done')
        })
        // .on('data', function (data:any, err: any) {
        //   log.debug(data)
        // })
        .on('error', function (err: any) {
          log.error(err)
          self.emit('error', new PluginError(PLUGIN_NAME, err));
        })
        .pipe(newTransformer(streamName))
        .on('error', function (err: any) {
          log.error(err)
          self.emit('error', new PluginError(PLUGIN_NAME, err));
        })

      // after our stream is set up (not necesarily finished) we call the callback
      log.debug('calling callback')    
      cb(returnErr, file);
    }

  })

  return strm
}