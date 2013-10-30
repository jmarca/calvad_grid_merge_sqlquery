/* global require console process it describe after before */

var should = require('should')

var async = require('async')
var _ = require('lodash')

var reduce = require('../lib/reduce')
var config_okay = require('../lib/config_okay')
var queries = require('../lib/query_postgres')
var get_hpms = queries.get_hpms_from_sql
var get_detector_routes = queries.get_detector_route_nums


var date = new Date()
var test_db_unique = date.getHours()+'-'+date.getMinutes()+'-'+date.getSeconds()+'-'+date.getMilliseconds()

var config
before(function(done){
    config_okay('test.config.json',function(err,c){
        config ={'postgres':c.postgres
                ,'couchdb':c.couchdb}

        return done()
    })
    return null
})


describe('make a map',function(){
    it('should get data from sql'
      ,function(done){
           var task={'cell_id':'189_72'
                    ,'year':2009
                    ,'options':{'postgres':config}
                    }
           async.parallel([
               function(cb){
                   get_hpms(task
                           ,function(err,cbtask){
                                // err should not exist
                                should.not.exist(err)
                                should.exist(cbtask)
                                cb(null,cbtask)
                            });
                   return null
               }
             ,function(cb){
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
                                          cb(null,cbtask)
                                      })
                  return null
              }
           ]
                         ,function(atask,btask){
                              // does js use references?
                              task.should.have.property('accum').with.lengthOf(8);
                              task.should.have.property('detector_route_numbers')
                              task.detector_route_numbers.should.have.length(1)

                              // call the merge code
                              reduce.post_process_sql_queries(task,function(err,cbtask){
                                  should.not.exist(err)
                                  should.exist(cbtask)
                                  task.should.have.property('class_map').with.lengthOf(1);
                                  task.should.have.property('aadt_store')

                              })
                          })

       })
})