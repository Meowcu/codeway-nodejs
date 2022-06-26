var path = require('path');
var env = require('dotenv').config();

 const {PubSub, Encodings, SchemaViews} = require('@google-cloud/pubsub');
 const {BigQuery} = require('@google-cloud/bigquery');
 const avro = require('avro-js');
 const projectId = 'codewayproject'
 const keyPubSub = env.GOOGLE_APPLICATION_CREDENTIALS;
 const topicNameOrId = 'PubSubTopic';
 const schemaNameOrId = 'CaseEventJSON';
 const location = 'US';
 const datasetId = 'Events';
 const tableId = 'User';
 // Creates a client; cache this for further use
 const pubSubClient = new PubSub({projectId,keyPubSub})
 const bigquery = new BigQuery({projectId,keyPubSub});

var username;
 module.exports.home = function(req,res){
   username = req.session.username;
   res.sendFile(path.join(__dirname,'../../home.html'));
}
module.exports.getactivity = function(req,res){
   try{
      getUsersActivity(req,res);
      }catch(e){
         console.log("GetActivity:" + e);
         res.send('Incorrect Activity');
      }
}

 module.exports.publishJSON = function(req,res){
   getUserInformation(username,req);
   req.session.username = username;
   res.redirect('/home');
}


async function getUsersActivity(req,res){
   //Total user count
   const query = `SELECT count(*) as TotalUserCount FROM \`${projectId}.${datasetId}.${tableId}\``;
   const options = {
       query: query,
       location: location 
   }
   const [job] = await bigquery.createQueryJob(options);
   const [rows] = await job.getQueryResults();
   
   var totalUserCount = rows[0]["TotalUserCount"];

   //new_user_count // average_session_duration //  active_user_count //date
   const tableIdGameEvents = 'GameEvents';
   const query1 = `SELECT first.Date,first.ActiveUserCount,second.DailyNewUser,third.AverageSessionDuration 
   FROM (SELECT COUNT(DISTINCT user_id) as ActiveUserCount, CAST(timestamp_millis(CAST(event_time AS INT64)) AS DATE) as Date FROM \`${projectId}.${datasetId}.${tableIdGameEvents}\` GROUP BY CAST(timestamp_millis(CAST(event_time AS INT64)) AS DATE)) first 
   RIGHT JOIN (SELECT COUNT(*) as DailyNewUser, CAST(timestamp_millis(CAST(insertdate AS INT64)) AS DATE) as Date 
   FROM \`${projectId}.${datasetId}.${tableId}\` GROUP BY CAST(timestamp_millis(CAST(insertdate AS INT64)) AS DATE)) second 
   ON first.Date = second.Date 
   RIGHT JOIN (SELECT AVG(diff_in_seconds) as AverageSessionDuration,Date 
   FROM (SELECT user_id,session_id,CAST(timestamp_millis(CAST(event_time AS INT64)) AS DATE) as Date,TIME_DIFF(MAX(CAST(timestamp_millis(CAST(event_time AS INT64)) AS TIME)),MIN(CAST(timestamp_millis(CAST(event_time AS INT64)) AS TIME)),SECOND) diff_in_seconds from \`${projectId}.${datasetId}.${tableIdGameEvents}\` group by session_id,user_id,CAST(timestamp_millis(CAST(event_time AS INT64)) AS DATE) ) GROUP BY Date) third 
   ON first.Date = third.Date`;
   const options1 = {
      query: query1,
      location: location 
  }
   const [job1] = await bigquery.createQueryJob(options1);
   const [rows1] = await job1.getQueryResults();

   var json = {
      total_user: totalUserCount,
      daily_stats: []
   };
   //push daily_stast
   for(var i in rows1){
      var item = rows1[i];
      json.daily_stats.push({
         "date" : item["Date"]["value"],
         "average_session_duration" : item["AverageSessionDuration"],
         "active_user_count" : item["ActiveUserCount"],
         "new_user_count" : item["DailyNewUser"]
      });
   }
   //console.log(JSON.stringify(json));
   //console.log(json);
   //const data = JSON.parse(json);
   console.log(json);
   res.render('activity',json);
}
async function getUserInformation(_username,req){
  
   try{
   const query = `SELECT * FROM \`${projectId}.${datasetId}.${tableId}\` WHERE username=@username`;
    const options = {
        query: query,
        location: location,
        params: {username: _username}
    }
    
    const [job] = await bigquery.createQueryJob(options);
    const [rows] = await job.getQueryResults();
    
    if(rows.length != 0){
     console.log(rows);
      
     // Encode the message.
     const EventData = {
      type: 'event',
      session_id: req.sessionID,
      event_name: req.body.publishEvent,
      event_time: Date.now(),
      page: 'home',
      country: rows[0]["country"],
      region: rows[0]["region"],
      city: rows[0]["city"],
      user_id: rows[0]["user_id"]
   };

   publishAvroRecords(topicNameOrId,EventData);
    return true;
   }
   else{
      return false;
   }
}catch(e){
   console.log("Exception" + e);
}
}


async function publishAvroRecords(topicNameOrId,EventData) {
  // Get the topic metadata to learn about its schema encoding.
  const topic = pubSubClient.topic(topicNameOrId);
  const [topicMetadata] = await topic.getMetadata();
  const topicSchemaMetadata = topicMetadata.schemaSettings;
  const schema = pubSubClient.schema(schemaNameOrId);
  if (!topicSchemaMetadata) {
    console.log(`Topic ${topicNameOrId} doesn't seem to have a schema.`);
    return;
  }
  const schemaEncoding = topicSchemaMetadata.encoding;
  console.log(__dirname);
  // Make an encoder using the official avro-js library.
  const definition = (await schema.get(SchemaViews.Full)).definition;
  const type = avro.parse(definition);
   var dataBuffer;
   switch (schemaEncoding) {
     case Encodings.Binary:
       dataBuffer = type.toBuffer(EventData);
       break;
     case Encodings.Json:
       dataBuffer = Buffer.from(type.toString(EventData));
       break;
     default:
       console.log(`Unknown schema encoding: ${schemaEncoding}`);
       break;
   }
   if (!dataBuffer) {
     console.log(`Invalid encoding ${schemaEncoding} on the topic.`);
     return;
   }
   
   const callback = (err, messageId) => {
      if (err) {
        console.log('MessageId: ' + messageId);
        return false;
      }
    };
   console.log("JSON Log Sample: " + JSON.stringify(EventData));
    //Publish JSON - AVRO Message
  topic.publish(dataBuffer, callback);
  console.log(`Avro Message published.`);
  return true;
}