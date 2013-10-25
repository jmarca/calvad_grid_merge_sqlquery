/* global require console process it describe after before */

var should = require('should')

var async = require('async')
var _ = require('lodash')
var get_hpms = require('../lib/query_postgres').get_hpms_from_sql
var fs = require('fs')


var env = process.env;
var puser = process.env.PSQL_USER
var ppass = process.env.PSQL_PASS
var phost = process.env.PSQL_TEST_HOST || '127.0.0.1'
var pport = process.env.PSQL_PORT || 5432

var options ={'host':phost
             ,'port':pport
             ,'username':puser
             ,'password':ppass
             }

describe('get_hpms_from_sql',function(){
    it('should get data from sql'
      ,function(done){
           var task={'cell_id':'189_72'
                    ,'year':2009
                    ,'options':options}
           get_hpms(task
                   ,function(err,cbtask){
                        // err should not exist
                        should.not.exist(err)
                        should.exist(cbtask)
                        cbtask.should.have.property('accum').with.lengthOf(5);
                        console.log(cbtask.accum)
                         done()
                     })
       })
})
