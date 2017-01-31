// a small server to run while testing the UI
var fs = require('fs')

var path = require('path')
var rootdir = path.normalize(__dirname)

var config_file = rootdir+'/config.json'
var config_okay=require('config_okay')

var optimist = require('optimist')
var year, jobs, filename
var queue = require('d3-queue').queue
var argv = optimist
           .usage('glob up HPMS records by grid for the given year.  Write a json file.\nUsage: $0')
           .options('j',{'default':4
                        ,'alias': 'jobs'
                        ,'describe':'How many simultaneous PSQL jobs to run, which will actually be doubled.  Default is 4'
                        })
           .options("h", {'alias':'help'
                         ,'describe': "display this hopefully helpful message"
                         ,'type': "boolean"
                         ,'default': false
                         })
           .options('y',{'demand':true
                        ,'alias':'year'
                        ,describe:'The year process.'
                        })
           .options('o',{'demand':false
                        ,'alias':'output'
                        ,describe:'The name of the output file.  Defaults to hpmsYYYY.json, where YYYY is the year provided.'
                        })
           .argv

if (argv.help){
    optimist.showHelp()
    process.exit()
}

year = argv.year
jobs = argv.jobs
filename = 'hpms'+year+'.json'
if(argv.o !== undefined){
    filename = argv.o
}
var all_hpms_handler = require('./lib/routes.js').all_hpms_handler
var output_file_name = argv.o
config_okay(config_file,function(err,config){
    if(err){
        console.log('error parsing config file.  check permissions are 0600',err)
        throw new Error(err)
    }
    all_hpms_handler(jobs,year,config,function(e,results){
        if(e){
            throw new Error(e)
        }
        fs.writeFile(filename
                     , JSON.stringify(results,null,' ')
                     , 'utf8'
                     , function(e,r){
                         if(e){
                             throw new Error(e)
                         }
                         console.log('done writing '+filename)
                         return null
                     })
        return null
    })
    return null
})
