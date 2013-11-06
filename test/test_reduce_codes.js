/* global require console process it describe after before */

var should = require('should')

var async = require('async')
var _ = require('lodash')

var reduce = require('../lib/reduce')
var config_okay = require('../lib/config_okay')
var queries = require('../lib/query_postgres')
var get_hpms_aadt = queries.get_hpms_from_sql
var get_detector_routes = queries.get_detector_route_nums


var date = new Date()
var test_db_unique = date.getHours()+'-'
                   + date.getMinutes()+'-'
                   + date.getSeconds()+'-'
                   + date.getMilliseconds()

var config
before(function(done){
    config_okay('test.config.json',function(err,c){
        config ={'postgres':c.postgres
                ,'couchdb':c.couchdb}

        return done()
    })
    return null
})

var yr = 2009

describe('post process sql queries',function(){
    it('should  handle a detectorized grid cell'
      ,function(done){
           var task={'cell_id':'189_72'
                    ,'year':yr
                    ,'options':config
                    }
           async.parallel([
               function(cb){
                   get_hpms_aadt(task
                           ,function(err,cbtask){
                                // err should not exist
                                should.not.exist(err)
                                should.exist(cbtask)
                                return cb(null,cbtask)
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
                                          _.each(cbtask.detector_route_numbers,function(row){
                                              _.keys(row).should.have.length(2)
                                              row.should.have.property('cell','189_72')
                                              row.should.have.property('route_number',101)
                                          });
                                          return cb(null,cbtask)
                                      })
                  return null
              }
           ]
                         ,function(atask,btask){
                              // does js use references?
                              task.should.have.property('accum').with.lengthOf(8);
                              task.should.have.property('detector_route_numbers')
                              task.detector_route_numbers.should.have.length(1)
                              // if those tests passed, then node.js
                              // is still pass by reference and is
                              // modifying refs in subroutines

                              //  call the merge code
                              reduce.post_process_sql_queries(task,function(err,cbtask){
                                  should.not.exist(err)
                                  should.exist(cbtask)
                                  task.should.have.property('class_map')
                                  _.size(task.class_map).should.eql(1)
                                  var route_classifications = _.keys(task.class_map)
                                  route_classifications.sort()
                                  route_classifications.should.eql(['101'])
                                  task.should.have.property('aadt_store')
                                  _.size(task.aadt_store).should.eql(3)
                                  //console.log(task.aadt_store)
                                  task.aadt_store.should.have.property('14')
                                  task.aadt_store['14']
                                  .should.have.keys('sum_aadt'
                                                   ,'sum_vmt'
                                                   ,'sum_lane_miles'
                                                   ,'sum_single_unit_mt'
                                                   ,'sum_combination_mt')
                                  task.aadt_store['14'].should.have.property( 'sum_aadt').with.approximately(185496,0.1)
                                  task.aadt_store['14'].should.have.property( 'sum_vmt').with.approximately(137844,0.1)
                                  task.aadt_store['14'].should.have.property( 'sum_lane_miles').with.approximately(25.69,0.01)
                                  task.aadt_store['14'].should.have.property( 'sum_single_unit_mt').with.approximately(4803,0.01)
                                  task.aadt_store['14'].should.have.property( 'sum_combination_mt').with.approximately(1005,0.01)

                                  task.aadt_store.should.have.property('16')
                                  task.aadt_store['16']
                                  .should.have.keys('sum_aadt'
                                                   ,'sum_vmt'
                                                   ,'sum_lane_miles'
                                                   ,'sum_single_unit_mt'
                                                   ,'sum_combination_mt')
                                  task.aadt_store['16'].should.have.property( 'sum_aadt').with.approximately(95491,0.1)
                                  task.aadt_store['16'].should.have.property( 'sum_vmt').with.approximately(102226,0.1)
                                  task.aadt_store['16'].should.have.property( 'sum_lane_miles').with.approximately(32.50,0.01)
                                  task.aadt_store.should.have.property('17')
                                  task.aadt_store['16'].should.have.property( 'sum_single_unit_mt').with.approximately(879,0.01)
                                  task.aadt_store['16'].should.have.property( 'sum_combination_mt').with.approximately(97,0.01)

                                  task.aadt_store['17']
                                  .should.have.keys('sum_aadt'
                                                   ,'sum_vmt'
                                                   ,'sum_lane_miles'
                                                   ,'sum_single_unit_mt'
                                                   ,'sum_combination_mt')
                                  task.aadt_store['17'].should.have.property( 'sum_aadt').with.approximately(153724,0.1)
                                  task.aadt_store['17'].should.have.property( 'sum_vmt').with.approximately(91443,0.1)
                                  task.aadt_store['17'].should.have.property( 'sum_lane_miles').with.approximately(54.03,0.01)
                                  task.aadt_store['17'].should.have.property( 'sum_single_unit_mt').with.approximately(235,0.01)
                                  task.aadt_store['17'].should.have.property( 'sum_combination_mt').with.approximately(0,0.01)

                                  return done()
                              })
                          })

       })
})
