var express = require('express')

var router = express.Router();

var controller = require('../controller/IndexController');

router.use(function(req,res,next){
    next();
});
router.get('/',controller.home);
router.post('/publishJSON',controller.publishJSON);
router.post('/getactivity',controller.getactivity);
module.exports = router;