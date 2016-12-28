/* global require console process it describe after before */

var should = require('should')

var queries = require('../lib/query_postgres')

var config_okay = require('config_okay')
var path = require('path')
var rootdir = path.normalize(__dirname)
var config_file = rootdir+'/../test.config.json'

var config
before(function(done){
    config_okay(config_file,function(err,c){
        config ={'postgresql':c.postgresql
                ,'couchdb':c.couchdb}

        return done()
    })
    return null
})


describe('get_hpms_from_sql (2014)',function(){
    it('should get data from sql, hpms_count_data table'
       ,function(done){
           var task={'cell_id':'189_72'
                     ,'year':2014
                     ,'options':config
                    }
           queries.get_hpms_from_sql_2014(task,function(err,cbtask){
               // err should not exist
               should.not.exist(err)
               should.exist(cbtask)
               cbtask.should.have.property('accum').with.lengthOf(6);
               cbtask.accum.forEach(function(row){
                   Object.keys(row).should.have.length(10)
                   row.should.have.property('cell','189_72')
                   row.should.have.property('year',2014)
                   row.should.have.property('f_system')
                   row.f_system.should.not.eql('NULL')
                   row.should.have.property('sum_aadt')
                   row.should.have.property('sum_vmt')
                   row.should.have.property('sum_single_unit')
                   row.should.have.property('sum_combination')
                   row.should.have.property('sum_single_unit_mt')
                   row.should.have.property('sum_combination_mt')
                   row.sum_single_unit_mt.should.eql(0)
                   row.sum_combination_mt.should.eql(0)
               })
               done()
           })
           return null
       })
    return null
})
describe('get_hpms_from_sql (2015)',function(){
    it('should get data from sql, new hpms_count_data table'
       ,function(done){
           var task={'cell_id':'189_72'
                     ,'year':2015
                     ,'options':config
                    }
           queries.get_hpms_from_sql_2014(task,function(err,cbtask){
               // err should not exist
               should.not.exist(err)
               should.exist(cbtask)
               cbtask.should.have.property('accum').with.lengthOf(6);
               cbtask.accum.forEach(function(row){
                   Object.keys(row).should.have.length(10)
                   row.should.have.property('cell','189_72')
                   row.should.have.property('year',2015)
                   row.should.have.property('f_system')
                   row.f_system.should.not.eql('NULL')
                   row.should.have.property('sum_aadt')
                   row.should.have.property('sum_vmt')
                   row.should.have.property('sum_single_unit')
                   row.should.have.property('sum_combination')
                   row.should.have.property('sum_single_unit_mt')
                   row.should.have.property('sum_combination_mt')
                   //console.log(row)
                   if(row.f_system=='CO P' ||
                      row.f_system=='Santa Barbara P' ||
                      row.f_system=='SHS_101_P' ||
                      row.f_system=='SHS_192_P' ||
                      row.f_system=='SHS_225_P'
                     ){
                       row.sum_single_unit_mt.should.be.above(0)
                       row.sum_combination_mt.should.be.above(0)
                   }else{
                       row.sum_single_unit_mt.should.eql(0)
                       row.sum_combination_mt.should.eql(0)
                   }
               })
               done()
           })
           return null
       })
    return null
})
