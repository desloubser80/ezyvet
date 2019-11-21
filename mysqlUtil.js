//generic mysql query executer. Can be used to connect to various systems(databases) by changing the config and  


const mysql = require('mysql');
const etlConfig = require('./etlConfig.json');

mysqlUtil = function runMysqlUtil()
{
    if (!(this instanceof mysqlUtil)) { return new mysqlUtil; }
    this.type = "mysqlUtil.js";
}

mysqlUtil.prototype.executeSql = function executeSql(sql,database)
{
    var self = this;
    return new Promise(async function(resolve,reject)
    {
        try
        {
            
            if(sql !== '')
            {
                
                var con = await self.getDBConnection(database)
                
                self.mysqlConnect(con,function (err)
                {
                    if(err)
                    {
                        throw(err);
                    }
                    else
                    {
                        self.mysqlQuery(con,sql,function(err,result,fields)
                        {
                            if(err)
                            {
                                self.mysqlDisconnect(con);
                               
                                throw(err);
                            }
                            self.mysqlDisconnect(con);
                            resolve(result);
                        });
                    }
                });
            }
            else
            {
               throw(err);
            }
        }
        catch (err)
        {
            if (con) con.end();
            reject(err);
        }
    });
}

mysqlUtil.prototype.getDBConnection = function getDBConnection(database)
{
    //can be used to get connections to various systems, will default to normal target system
   
    var self = this;
    return new Promise(function (resolve,reject)
    {
        
        try
        {
            
            if(database === "default")
            {
                var host = etlConfig.defaultCredentials.host;
                var database = etlConfig.defaultCredentials.database;
                var user = etlConfig.defaultCredentials.user;
                var password = etlConfig.defaultCredentials.password;
                var port = etlConfig.defaultCredentials.port;
            }else{
                var host = etlConfig.defaultCredentials.host;
                var database = etlConfig.defaultCredentials.database;
                var user = etlConfig.defaultCredentials.user;
                var password = etlConfig.defaultCredentials.password;
                var port = etlConfig.defaultCredentials.port;
            }
            
               
            var con = mysql.createConnection
            ({
                host :host
                ,user : user
                ,password: password
                ,database: database
                ,port : port
            });
            resolve (con);
        }
        catch(e)
        {
            reject(e);
        }
    });
}

mysqlUtil.prototype.mysqlConnect = function mysqlConnect(connection,done)
{
    connection.connect(done);
}

mysqlUtil.prototype.mysqlDisconnect = function mysqlDisconnect(connection)
{
    if(connection) connection.end();
}

mysqlUtil.prototype.mysqlQuery = function mysqlQuery(connection,sql,done)
{
    connection.query(sql,done);
}

module.exports =  mysqlUtil





