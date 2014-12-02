/* global require console process it describe after before */

var should = require('should')

var async = require('async')
var _ = require('lodash')

var reduce = require('../lib/reduce')
var config_okay = require('config_okay')
var path = require('path')
var rootdir = path.normalize(__dirname)
var config_file = rootdir+'/../test.config.json'

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
    config_okay(config_file,function(err,c){
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
                         ,function(err,results){
                              var atask = results[0]
                              var btask = results[1]
                              // does js use references?
                              //console.log('atask'+atask.accum.length)
                              //console.log('btask'+btask.accum.length)
                              // yes it does

                              // for some reason, when working over a
                              // slow DB connection, this test will
                              // fail when running mocha on all tests
                              // at once, but not when running mocha
                              // on just this test
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
                                  .should.have.keys('sum_vmt'
                                                   ,'sum_lane_miles'
                                                   ,'sum_single_unit_mt'
                                                   ,'sum_combination_mt')
                                  task.aadt_store['14'].should.have.property( 'sum_vmt').with.approximately((93787+16001+3747),0.1)
                                  task.aadt_store['14'].should.have.property( 'sum_lane_miles').with.approximately((17.15+2.6+.34),0.01)
                                  task.aadt_store['14'].should.have.property( 'sum_single_unit_mt').with.approximately((3383+480+0),0.01)
                                  task.aadt_store['14'].should.have.property( 'sum_combination_mt').with.approximately((950+0+0),0.01)

                                  task.aadt_store.should.have.property('16')
                                  task.aadt_store['16']
                                  .should.have.keys('sum_vmt'
                                                   ,'sum_lane_miles'
                                                   ,'sum_single_unit_mt'
                                                   ,'sum_combination_mt')
                                  task.aadt_store['16'].should.have.property( 'sum_vmt').with.approximately((4507+51935),0.1)
                                  task.aadt_store['16'].should.have.property( 'sum_lane_miles').with.approximately((2.9+11.37),0.01)
                                  task.aadt_store['16'].should.have.property( 'sum_single_unit_mt').with.approximately((90+306),0.01)
                                  task.aadt_store['16'].should.have.property( 'sum_combination_mt').with.approximately((0+97),0.01)

                                  task.aadt_store.should.have.property('17')
                                  task.aadt_store['17']
                                  .should.have.keys('sum_vmt'
                                                   ,'sum_lane_miles'
                                                   ,'sum_single_unit_mt'
                                                   ,'sum_combination_mt')
                                  task.aadt_store['17'].should.have.property( 'sum_vmt').with.approximately(61562,0.1)
                                  task.aadt_store['17'].should.have.property( 'sum_lane_miles').with.approximately(39.82,0.01)
                                  task.aadt_store['17'].should.have.property( 'sum_single_unit_mt').with.approximately(130,0.01)
                                  task.aadt_store['17'].should.have.property( 'sum_combination_mt').with.approximately(0,0.01)

                                  return done()
                              })
                          })

       })
})

/**
 * The tests were developed as follows.  first run the query on the bare db, and you get
 *
  cell  | year | route_number | f_system | sum_aadt | sum_vmt |  sum_lane_miles   | sum_single_unit | sum_single_unit_mt | sum_combination | sum_combination_mt
--------+------+--------------+----------+----------+---------+-------------------+-----------------+--------------------+-----------------+--------------------
 189_72 | 2009 | 101          |        2 |    29000 |   20119 |  2.77508869053543 |               0 |                  0 |               0 |                  0
 189_72 | 2009 | 101          |       12 |   267000 |   95487 |  4.29814414732388 |            6670 |               2581 |            8000 |               3252
 189_72 | 2009 |              |       14 |   151196 |   93787 |   17.150024415496 |            5098 |               3383 |            1381 |                950
 189_72 | 2009 | 192          |       14 |    12300 |   16001 |  2.60181767868063 |             369 |                480 |               0 |                  0
 189_72 | 2009 | 225          |       14 |    22000 |    3747 | 0.340672814821636 |               0 |                  0 |               0 |                  0
 189_72 | 2009 | 192          |       16 |     3100 |    4507 |  2.90801266914361 |              62 |                 90 |               0 |                  0
 189_72 | 2009 |              |       16 |    92391 |   51935 |  11.3696098361967 |            1034 |                306 |             240 |                 97
 189_72 | 2009 |              |       17 |   153724 |   61562 |   39.826748623163 |             547 |                130 |               0 |                  0
(8 rows)

 * I want to rip out 101, as it is tracked already.
 *
 * so summing, you should get
 *
 * f_system 2 should not exist
 * f_system 12 should not exist
 *
 * the others should be unchanged.
 *
 *
 */