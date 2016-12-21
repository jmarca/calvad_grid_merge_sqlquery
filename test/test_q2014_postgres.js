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

var yr = 2014

describe('get_hpms_from_sql (2014)',function(){
    it('should get data from sql, 2014 table'
       ,function(done){
           var task={'cell_id':'189_72'
                     ,'year':yr
                     ,'options':config
                    }
           queries.get_hpms_from_sql_2014(task,function(err,cbtask){
               // err should not exist
               should.not.exist(err)
               should.exist(cbtask)
               cbtask.should.have.property('accum').with.lengthOf(6);
               cbtask.accum.forEach(function(row){
                   Object.keys(row).should.have.length(6)
                   row.should.have.property('cell','189_72')
                   row.should.have.property('year',yr)
                   row.should.have.property('f_system')
                   row.f_system.should.not.eql('NULL')
                   row.should.have.property('sum_aadt')
                   row.should.have.property('sum_vmt')
               })
               done()
           })
           return null
       })
    return null
})
