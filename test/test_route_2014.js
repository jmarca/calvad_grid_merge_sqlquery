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
testport += 3

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

describe('2014 works okay',function(){
    var yr = 2014
    it('should  handle a detectorized grid cell'
       ,function(done){
           var task={'i':'189',j:'72'
                     ,'year': yr
                    }
           var totals = {}
           totals['CO P'] = 281.01 +    395.49+  13691.23+    754.86+   2523.79+   2840.37
                  +  183.91+    578.60+   2173.54+    286.70+   2246.23+   8859.82
                  +  8072.84+   7583.76+   7003.85+  12687.84+    583.20

          totals.RAMP = 833.82 +   585.70+  1629.81
          totals['Santa Barbara P'] =  495.63+   5154.38+    324.79+   4811.05
                  +   1908.75+   2776.95+    211.07+    829.35+   1461.81
                  +    753.23+   2656.43+    338.13+    218.48+   1410.18
                  +    355.70+    715.37+    708.15+   1539.90+   2385.83
                  +    415.57+    143.11+    261.51+    115.61+    183.84
                  +   1106.77+    869.01+     94.77+   1162.73+   5112.97
                  +   1770.65+    226.28+    169.83+    325.61+   2676.68
                  +    929.94+     48.89+  10009.80+   6885.40+    284.38
                  +    113.10+   1235.91+   1244.04+     89.05+    368.68
                  +   1034.40+      5.97+   1017.93+   2058.92+     64.85
                  +    542.68+    776.54+     48.87+     64.57+    141.33
                  +     59.76+    190.46+    478.43+    281.58+   2332.50
                  +    357.62+  10506.20+     24.45+    188.08+    640.49
                  +    599.45+    259.70+   1257.88+   4953.68+    695.33
                  +    551.14+    502.98+     36.90+     30.97+     96.87
                  +    193.09+   1145.04+   3498.88+   2437.06+  10799.26
                  +    794.18+    425.00+    174.31+     62.61+   3457.36
                  +   1965.83+    255.39+   1761.90+      5.64+     36.83
                  +   3037.69+    870.82+    971.80+    861.59+    356.90
                  +    193.94+    171.00+  17044.41+    516.62+    926.45
                  +   3085.77+   2037.21
           totals['SHS_101']  =  78828.22
           totals['SHS_101'] += 104180.62
           totals['SHS_192']  =   4986.85
           totals['SHS_192'] +=  12212.54
           totals['SHS_225']  =   2301.08

           totals['totals'] = (Object.keys(totals)).reduce(function(a,b){
               return a+Math.round(totals[b])
           },0)
           totals['totals'] -= totals['SHS_101']
           //console.log(totals)
          request.get(server_host
                      +'/'
                      +'hpms/data/'
                      +task.year+'/'
                      +task.i+'/'
                      +task.j+'.json'
                      ,function(e,r,b){
                          var f_systems = {}
                          should.not.exist(e)
                          should.exist(r)
                          should.exist(b)

                          var doc = JSON.parse(b)


                          doc.should.have.lengthOf(6)

                          doc.forEach(function(d){
                              d.should.have.keys('sum_vmt'
                                                 ,'sum_lane_miles'
                                                 ,'sum_single_unit_mt'
                                                 ,'sum_combination_mt'
                                                 ,'f_system'
                                                 ,'road_type'
                                                 //,'classify'
                                                 ,'year'
                                                 ,'cell_i'
                                                 ,'cell_j')

                              f_systems[d.f_system] = 1
                              //console.log(d)
                              d.sum_vmt.should.be.approximately(totals[d.f_system],1)
                              return null
                          })
                          f_systems.should.eql({ "CO P" : 1
                                                 ,"RAMP": 1
                                                 ,"SHS_192": 1
                                                 ,"SHS_225": 1
                                                 ,"Santa Barbara P": 1
                                                 ,"totals": 1
                                               })



                          doc[doc.length-1].should.have.property('f_system','totals')
                          doc[doc.length-1].should.have.property('road_type','totals')
                          return done()
                      })
          return null
      })
    // it('should  handle an empty cell'
    //    ,function(done){
    //        var task={'i':'100',j:'216'
    //                  ,'year': yr
    //                 }

    //        request.get(server_host
    //                    +'/'
    //                    +'hpms/data/'
    //                    +task.year+'/'
    //                    +task.i+'/'
    //                    +task.j+'.json'
    //                    ,function(e,r,b){
    //                        should.not.exist(e)
    //                        should.exist(r)
    //                        should.exist(b)
    //                        var doc = JSON.parse(b)
    //                        doc.should.have.lengthOf(0)

    //                        return done()
    //                    })
    //        return null
    //    })
    return null

})

// describe('using exported hpms_data handler, my own route',function(){
//     var yr = 2014
//     it('should  handle a detectorized grid cell'
//       ,function(done){
//            var task={'i':'189',j:'72'
//                     ,'year': yr
//                     }

//            request.get(server_host
//                       +'/'
//                       +'hpms/datatoo/'
//                       +task.j+'/'
//                       +task.i+'/'
//                       +task.year
//                       +'.json'
//                       ,function(e,r,b){
//                            should.not.exist(e)
//                            should.exist(r)
//                            should.exist(b)

//                            var doc = JSON.parse(b)


//                         doc.should.have.lengthOf(4)
//                            doc[0].should.have.keys('sum_vmt'
//                                             ,'sum_lane_miles'
//                                             ,'sum_single_unit_mt'
//                                             ,'sum_combination_mt'
//                                             ,'f_system'
//                                             ,'road_type'
//                                                   ,'year'
//                                                   ,'cell_i'
//                                                   ,'cell_j'
//                                                   )
//                            doc[0].should.have.property('f_system','14')
//                         doc[0].should.have.property( 'sum_vmt').with.approximately((93787+16001+3747),0.1)
//                         doc[0].should.have.property( 'sum_lane_miles').with.approximately((17.15+2.6+.34),0.01)
//                         doc[0].should.have.property( 'sum_single_unit_mt').with.approximately((3383+480+0),0.01)
//                         doc[0].should.have.property( 'sum_combination_mt').with.approximately((950+0+0),0.01)
//                            doc[3].should.have.property('f_system','totals')
//                            doc[3].should.have.property('road_type','totals')

//                         return done()
//                     })
//         return null
//     })
// })

// describe('dataonly route route',function(){
//     var yr = 2014
//     it('should  handle a detectorized grid cell'
//       ,function(done){
//            var task={'i':'189',j:'72'
//                     ,'year': yr
//                     }

//            request.get(server_host
//                       +'/'
//                       +'hpms/dataonly/'
//                       +task.year+'/'
//                       +task.i+'/'
//                       +task.j+'.json'
//                       ,function(e,r,b){
//                            should.not.exist(e)
//                            should.exist(r)
//                            should.exist(b)

//                            var doc = JSON.parse(b)


//                         doc.should.have.lengthOf(6)
//                         doc[0].should.have.keys('sum_vmt'
//                                             ,'sum_lane_miles'
//                                             ,'sum_single_unit_mt'
//                                             ,'sum_combination_mt'
//                                             ,'f_system'
//                                                ,'road_type'
//                                                ,'year'
//                                                ,'cell_i'
//                                                ,'cell_j'
//                                                )
//                            doc[0].should.have.property('f_system','2')
//                         doc[0].should.have.property( 'sum_vmt').with.approximately(20119,0.1)
//                         doc[0].should.have.property( 'sum_lane_miles').with.approximately(2.77,0.01)
//                         doc[0].should.have.property( 'sum_single_unit_mt').with.approximately(0,0.01)
//                         doc[0].should.have.property( 'sum_combination_mt').with.approximately(0,0.01)

//                            doc[2].should.have.property('f_system','14')
//                         doc[2].should.have.property( 'sum_vmt').with.approximately((93787+16001+3747),0.1)
//                         doc[2].should.have.property( 'sum_lane_miles').with.approximately((17.15+2.6+.34),0.01)
//                         doc[2].should.have.property( 'sum_single_unit_mt').with.approximately((3383+480+0),0.01)
//                         doc[2].should.have.property( 'sum_combination_mt').with.approximately((950+0+0),0.01)

//                            doc[5].should.have.property('f_system','totals')
//                            doc[5].should.have.property('road_type','totals')
//                            doc[5].should.have.property( 'sum_vmt').with.approximately((20119+95487+93787+16001+3747+4507+51935+61562),0.1)

//                         return done()
//                     })
//         return null
//     })
// })
