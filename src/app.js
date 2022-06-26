//Express
var express = require('express') // server environments
var path = require('path') //directory path usage
var app = express(); // req/res 
const session = require('express-session');
var uuid = require('uuid');
const bodyParser = require('body-parser'); // middleware

var routerIndex = require('./app_server/routes/indexRouter');
var authIndex = require('./app_server/routes/authRouter');

app.set('view engine', 'ejs');
app.use('/public',express.static(path.join(__dirname,'public'))); //public directory accessable
app.use('/views',express.static(path.join(__dirname,'views'))); //public directory accessable

app.use(session({
	secret: 'secret',
	resave: true,
	saveUninitialized: true
}));
app.use(bodyParser.urlencoded({ extended: false }));


app.use(function(req,res,next){
    next();
});



app.use('/',authIndex);
app.use('/home',routerIndex);

app.listen(8080);