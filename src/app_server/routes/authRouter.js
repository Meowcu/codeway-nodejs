var express = require('express')
var authRouter = express.Router();
var authController = require('../controller/AuthController');
var env = require('dotenv').config();

authRouter.use(function(req,res,next){
    next();
});

authRouter.get('/',authController.index);
authRouter.get('/login',authController.login);
authRouter.get('/createUser',authController.createUser);
//Authorization
const {BigQuery} = require('@google-cloud/bigquery');
const location = 'US';
const projectId = 'codewayproject'
const keyPubSub = env.GOOGLE_APPLICATION_CREDENTIALS;
const bigquery = new BigQuery({projectId,keyPubSub});
const datasetId = 'Events';
const tableId = 'User';
var uuid = require('uuid');

authRouter.post('/auth',function(req,res){
    let username = req.body.username;
    let password = req.body.password;
    if(username && password){
        authUser(username, password,req,res);
    } 
});


authRouter.post('/create',function(req,res){
    let _username = req.body.username;
    let _password = req.body.password;
    let _name = req.body.name;
    let _surname = req.body.surname;
    let _country = req.body.country;
    let _region = req.body.region;
    let _city = req.body.city;
    let _user_id = uuid.v1();
    const result = insertUser(_username,_password,_name,_surname,_country,_region,_city,_user_id);
    if(result){
        //res.end("User created.");
        res.redirect('/login');
    }
    else
        //res.end("User Creation has been failure.");
        res.redirect('/createUser');
    res.end();
})
module.exports = authRouter;

async function authUser(_username,_password,req,res){
    const query = `SELECT * FROM \`${projectId}.${datasetId}.${tableId}\` WHERE username=@username AND password=@password`;
    const options = {
        query: query,
        location: location,
        params: {username: _username,password: _password}
    }

    const [job] = await bigquery.createQueryJob(options);
    console.log('Job started.');

    const [rows] = await job.getQueryResults();

    if(rows.length != 0){
        console.log(rows.length);
        req.session.loggedin = true;
        req.session.username = _username;
        res.redirect('/home');
    }
    else{
        res.send('Incorrect Username and/or Password!');
    }
}

async function insertUser(_username,_password,_name,_surname,_country,_region,_city,_user_id){
    console.log("insert user");
        const rows = [
            {username: _username, password: _password, name: _name, surname: _surname, country: _country, region: _region,city:_city, user_id: _user_id, insertdate: Date.now(),activityTime: Date.now()}
        ];

    try{
        await bigquery.dataset(datasetId).table(tableId).insert(rows);
        console.log('Row inserted');
    }catch(e){
        console.error(JSON.stringify(e,null,2));
        return false;
    }
    return true;
}