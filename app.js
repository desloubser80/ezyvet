 
const mysqlClass = require('./mysqlUtil.js');
const mysqlUtils = mysqlClass();
const etlConfig = require('./etlConfig.json');
const fs = require('fs');

var counter = 0;
var insertSize = etlConfig.insertSize;
var csvFile = etlConfig.csv.filepath;



async function run(){

	if (etlConfig.csv.active === true){
		try{
			result = await readFile()
			console.log("Result: ",result)
		}catch(e){
			console.log(e)
		}
	}
}

//the table into which you want to insert and the databse to insert into must be used with the data. 
//This allows for inserts into multiple tables/databases, using the same method
async function readFile(){
	return new Promise(async function(resolve,reject){
			try{
				//console.log(csvFile)
				fs.readFile(etlConfig.csv.filepath,"utf8", async function(err,data){
					data = data.split("\n")
					var result = await processData(data,'','default')
					resolve(result)
				})
			}catch(e){
				reject(e)
			}
	})
}

//data must be an array of item representing the columns of the the table to insert into allwoing various sources to use this method,
//TO IMPLEMENT: table is the table to insert into
//database is the database in which the table exists
//TO IMPLEMENT: bulk inserts are faster in mysql, so build up a big insert string before sending. Size of insert string configurable in etlConfig. Smaller inserts are easier to debug
async function processData(data,table,database){
	return new Promise(async function(resolve,reject){
		try{
			var valueString = ''
			var count = 0
			
			data.forEach(async function(item){
				//introduce flag here to handle both files with headers and files without
				if (count > 0){
					var itemArray = item.split(",")
					
					// //CONTACT
					var title = titleScrubber(itemArray[1])
										
					//names
					var first_name = nameScrubber(itemArray[2])
					var last_name = nameScrubber(itemArray[3])
										
					//dob
					date_of_birth = dobScrubber(itemArray[4])
										
					//company
					var company_name = itemArray[0]
					if(company_name){company_name = companyNameScrubber(company_name)}
															
					//notes
					var notes = itemArray[15]
					if(notes){notes = noteScrubber(notes)}
						
					//Insert contact into sql	
					var sql = `INSERT INTO contact(title,first_name,last_name,company_name,date_of_birth,notes) VALUES ("${title}","${first_name}","${last_name}","${company_name}","${date_of_birth}","${notes}")`
					var result = await executeSql(sql,database)
					var contact_id = result.insertId

					//ADDRESS
					addressScrubber(contact_id, itemArray[5],itemArray[6],itemArray[7],itemArray[8],itemArray[9])
					
					//PHONE
					var home = itemArray[10]
					if(home && home !== ''){
						home = home.replace(/[)(-]/g,'')
						var type = 'Home'
						phoneNumberScrubber(home,type,contact_id,first_name)
					}

					var fax = itemArray[11]
					if(fax && fax !== ''){
						fax = fax.replace(/[)(-]/g,'')
						var type = 'Other'
						phoneNumberScrubber(fax,type,contact_id,first_name)
					}
					
					var work = itemArray[12]
					if(work && work !== ''){
						work = work.replace(/[)(-]/g,'')
						var type = 'Work'
						phoneNumberScrubber(work,type,contact_id,first_name)
					}
					
					var mobile = itemArray[13]
					if(mobile && mobile !== ''){
						mobile = mobile.replace(/[)(-]/g,'')
						var type = 'Mobile'
						phoneNumberScrubber(mobile,type,contact_id,first_name)
					}
					
					var other = itemArray[14]
					if(other && other !== ''){
						other = other.replace(/[)(-]/g,'')
						var type = 'Other'
						phoneNumberScrubber(other,type,contact_id,first_name)
					}
				}
				else{
					var headers = item
				}
				count ++ 
			})
			//var result = executeSql(sql,database)
			var result = "done"
			resolve(result)
		}
		catch(e){
			reject(e)
		}
	})
}

async function phoneNumberScrubber(number,type,contact_id,first_name){
	if (type === 'Mobile'){
		if(number.substr(0,2) !== "09" && number.substr(0,2) !== "64"){
			number = "64"+number
		}else if (number.substr(0,2) == "09"){
			type = 'Home'
		}
		sql = `INSERT INTO phone (contact_id,name,content,type) VALUES (${contact_id},"${first_name}","${number}","${type}")`
		var result = await executeSql(sql)
	}
	else{
		if(number.substr(0,2) !== "64" && number.substr(0,2) !== "09"){
			number = "09"+number
		}else if (number.substr(0,2) == "64"){
			type = 'Mobile'
		}
		sql = `INSERT INTO phone (contact_id,name,content,type) VALUES (${contact_id},"${first_name}","${number}","${type}")`
		var result = await executeSql(sql)
	}
}

async function addressScrubber(contact_id,street_1,street_2,suburb,city,post_code){
	sql  = `INSERT INTO address (contact_id, street1,street2, suburb, city, post_code) VALUES (${contact_id}, "${street_1}", "${street_2}", "${suburb}", "${city}", "${post_code}")`
	var result = await executeSql(sql)
}

function noteScrubber(notes){
	notes = notes.replace(/[";]/g,'')
	notes = notes.replace(/\u00a9|\u00ae|[\u2000-\u3300]|\ud83c[\ud000-\udfff]|\ud83d[\ud000-\udfff]|\ud83e[\ud000-\udfff]/g,'')
	return notes
}

function companyNameScrubber(company_name){
	var company_parts = company_name.split(' ')
	for (var i = 0; i<company_parts.length ;i++){
	 	var index = company_parts[i].indexOf('.')
	 	if (index > 0 ){company_parts[i] = company_parts[i].toUpperCase()}
	}
	company_name = company_parts.join(' ')
	company_name = company_name.replace(/["]/g,"")
	return company_name
}

function dobScrubber(date_of_birth){
	if(date_of_birth){
		date_of_birth = date_of_birth.replace(/[-]/g,"/")
		var date_parts = date_of_birth.split('/')
		var month = date_parts[0] -1   //javascript months are zero indexed
		var day = parseInt(date_parts[1]) 
		var year = parseInt(date_parts[2])
		if (year < 100){
			year = parseInt("19"+year)
		}  
		date_of_birth = new Date(year,month,day)
		date_of_birth = date_of_birth.toISOString().replace(/[\"T\"]/g,' ')
		date_of_birth = date_of_birth.replace(/[\"Z\"]/g,'')
	}else{
		date_of_birth = "0000-00-00 00:00:00"
	}
	return(date_of_birth)
}

function nameScrubber(name){
	if(name){
		name = name.replace(/[']/g,"\'")
		var first_letter = name.substr(0,1)
		first_letter = first_letter.toUpperCase()
		name = first_letter  + name.substr(1,name.length)
	 }else{
	 	name = ''
	 }
	return name
}

function titleScrubber(title){
	if(title){
		title = title.replace(/\./g,'')
		if(title.toLowerCase() == 'mr'){
			title = "Mr"
		}else if (title.toLowerCase() == 'mrs'){
			title = "Mrs"
		}else if (title.toLowerCase() == 'miss'){
			title = "Miss"
		}else if (title.toLowerCase() == 'ms'){
			title = "Ms"
		}else if (title.toLowerCase() == 'dr'){
			title = "Dr"
		}else{
			title = "Unknown"
		}
	}else{
		title = "Unknown"
	}
	return title
}

async function executeSql(sql,database){
	return new Promise(async function(resolve,reject){
		try{
			
			var result = await mysqlUtils.executeSql(sql,database)
			resolve(result) 
		}catch(e){
			reject(e)
		}
	})
}

run()
 	



