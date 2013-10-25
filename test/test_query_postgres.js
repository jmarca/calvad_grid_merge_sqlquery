/* global require console process it describe after before */

var should = require('should')

var async = require('async')
var _ = require('lodash')
var get_hpms = require('../lib/query_postgres').get_hpms_from_sql
var get_detector_routes = require('../lib/query_postgres').get_detector_route_nums
var fs = require('fs')


var env = process.env;
var puser = process.env.PSQL_USER
var ppass = process.env.PSQL_PASS
var phost = process.env.PSQL_TEST_HOST || '127.0.0.1'
var pport = process.env.PSQL_PORT || 5432
var db    = process.env.PSQL_DB   || 'spatialvds'

var options ={'host':phost
             ,'port':pport
             ,'username':puser
             ,'password':ppass
             ,'db':db
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
                        cbtask.should.have.property('accum').with.lengthOf(8);
                        _.each(cbtask.accum,function(row){
                            _.keys(row).should.have.length(7)
                            row.should.have.property('cell','189_72')
                            row.should.have.property('year',2009)
                            row.should.have.property('f_system')
                            row.f_system.should.be.within(1, 19)
                            row.should.have.property('route_number')
                            row.should.have.property('sum_aadt')
                            row.should.have.property('sum_vmt')
                            row.should.have.property('sum_lane_miles')

                        });
                        done()
                    })
       })
})


describe('get_detector_route_nums',function(){
    it('should get data from sql'
      ,function(done){
           var task={'cell_id':'189_72'
                    ,'year':2009
                    ,'options':options}
           get_detector_routes(task
                   ,function(err,cbtask){
                        // err should not exist
                        should.not.exist(err)
                        should.exist(cbtask)
                        cbtask.should.have.property('detector_route_numbers')
                        cbtask.detector_route_numbers.should.have.length(1)
                        _.each(cbtask.accum,function(row){
                            _.keys(row).should.have.length(2)
                            row.should.have.property('cell','189_72')
                            row.should.have.property('route_number',101)
                        });
                        done()
                     })
       })
})