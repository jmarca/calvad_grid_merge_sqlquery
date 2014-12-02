var should = require('should')
var express = require('express')

var path    = require('path')
var rootdir = path.normalize(__dirname)
var config_file = rootdir+'/../test.config.json'
var config_okay=require('config_okay')

var app,server
var env = process.env;
var testhost = env.TEST_HOST || '127.0.0.1'
var testport = env.TEST_PORT || 3000
testport += 2

var routes = require('../.')
var hpms_data_route = routes.hpms_data_route
var hpms_data_nodetectors_route = routes.hpms_data_nodetectors_route

var request = require('request')


var server_host = 'http://'+testhost + ':'+testport


var my_handler = function(config,app){
    app.get('/hpms/datatoo/:j/:i/:yr.:format?'
           ,function(req,res,next){
                var task={'cell_id':req.params.i+'_'+req.params.j
                         ,'cell_i':req.params.i
                         ,'cell_j':req.params.j
                         ,'year':req.params.yr}
                task.options=config
                routes.hpms_data_handler(task,function(err,result){
                    if(err) return next(err)
                    res.json(result)
                    return null
                })
                return null
            })

}


before(
    function(done){

        config_okay(config_file,function(err,c){
            app = express()

            hpms_data_route(c,app)
            hpms_data_nodetectors_route(c,app)
            my_handler(c,app)
            app.listen(testport,testhost,done)

        })
    }
)

describe('basic hpms_data route',function(){
    var yr = 2009
    it('should  handle a detectorized grid cell'
      ,function(done){
           var task={'i':'189',j:'72'
                    ,'year': yr
                    }

           request.get(server_host
                      +'/'
                      +'hpms/data/'
                      +task.year+'/'
                      +task.i+'/'
                      +task.j+'.json'
                      ,function(e,r,b){
                           should.not.exist(e)
                           should.exist(r)
                           should.exist(b)

                           var doc = JSON.parse(b)


                        doc.should.have.lengthOf(4)
                           doc[0].should.have.keys('sum_vmt'
                                            ,'sum_lane_miles'
                                            ,'sum_single_unit_mt'
                                            ,'sum_combination_mt'
                                            ,'f_system'
                                            ,'road_type'
                                                  ,'year'
                                                  ,'cell_i'
                                                  ,'cell_j'
                                                  )
                           doc[0].should.have.property('f_system','14')
                        doc[0].should.have.property( 'sum_vmt').with.approximately((93787+16001+3747),0.1)
                        doc[0].should.have.property( 'sum_lane_miles').with.approximately((17.15+2.6+.34),0.01)
                        doc[0].should.have.property( 'sum_single_unit_mt').with.approximately((3383+480+0),0.01)
                        doc[0].should.have.property( 'sum_combination_mt').with.approximately((950+0+0),0.01)
                           doc[3].should.have.property('f_system','totals')
                           doc[3].should.have.property('road_type','totals')
                        return done()
                    })
        return null
    })
})

describe('using exported hpms_data handler, my own route',function(){
    var yr = 2009
    it('should  handle a detectorized grid cell'
      ,function(done){
           var task={'i':'189',j:'72'
                    ,'year': yr
                    }

           request.get(server_host
                      +'/'
                      +'hpms/datatoo/'
                      +task.j+'/'
                      +task.i+'/'
                      +task.year
                      +'.json'
                      ,function(e,r,b){
                           should.not.exist(e)
                           should.exist(r)
                           should.exist(b)

                           var doc = JSON.parse(b)


                        doc.should.have.lengthOf(4)
                           doc[0].should.have.keys('sum_vmt'
                                            ,'sum_lane_miles'
                                            ,'sum_single_unit_mt'
                                            ,'sum_combination_mt'
                                            ,'f_system'
                                            ,'road_type'
                                                  ,'year'
                                                  ,'cell_i'
                                                  ,'cell_j'
                                                  )
                           doc[0].should.have.property('f_system','14')
                        doc[0].should.have.property( 'sum_vmt').with.approximately((93787+16001+3747),0.1)
                        doc[0].should.have.property( 'sum_lane_miles').with.approximately((17.15+2.6+.34),0.01)
                        doc[0].should.have.property( 'sum_single_unit_mt').with.approximately((3383+480+0),0.01)
                        doc[0].should.have.property( 'sum_combination_mt').with.approximately((950+0+0),0.01)
                           doc[3].should.have.property('f_system','totals')
                           doc[3].should.have.property('road_type','totals')

                        return done()
                    })
        return null
    })
})

describe('more basic hpms_data route',function(){
    var yr = 2009
    it('should  handle a detectorized grid cell'
      ,function(done){
           var task={'i':'189',j:'72'
                    ,'year': yr
                    }

           request.get(server_host
                      +'/'
                      +'hpms/dataonly/'
                      +task.year+'/'
                      +task.i+'/'
                      +task.j+'.json'
                      ,function(e,r,b){
                           should.not.exist(e)
                           should.exist(r)
                           should.exist(b)

                           var doc = JSON.parse(b)


                        doc.should.have.lengthOf(6)
                        doc[0].should.have.keys('sum_vmt'
                                            ,'sum_lane_miles'
                                            ,'sum_single_unit_mt'
                                            ,'sum_combination_mt'
                                            ,'f_system'
                                               ,'road_type'
                                               ,'year'
                                               ,'cell_i'
                                               ,'cell_j'
                                               )
                           doc[0].should.have.property('f_system','2')
                        doc[0].should.have.property( 'sum_vmt').with.approximately(20119,0.1)
                        doc[0].should.have.property( 'sum_lane_miles').with.approximately(2.77,0.01)
                        doc[0].should.have.property( 'sum_single_unit_mt').with.approximately(0,0.01)
                        doc[0].should.have.property( 'sum_combination_mt').with.approximately(0,0.01)

                           doc[2].should.have.property('f_system','14')
                        doc[2].should.have.property( 'sum_vmt').with.approximately((93787+16001+3747),0.1)
                        doc[2].should.have.property( 'sum_lane_miles').with.approximately((17.15+2.6+.34),0.01)
                        doc[2].should.have.property( 'sum_single_unit_mt').with.approximately((3383+480+0),0.01)
                        doc[2].should.have.property( 'sum_combination_mt').with.approximately((950+0+0),0.01)

                           doc[5].should.have.property('f_system','totals')
                           doc[5].should.have.property('road_type','totals')
                           doc[5].should.have.property( 'sum_vmt').with.approximately((20119+95487+93787+16001+3747+4507+51935+61562),0.1)

                        return done()
                    })
        return null
    })
})
