var path = require('path');
var express = require('express');

module.exports.index = function(req,res){
    res.sendFile(path.join(__dirname,'../../index.html'));
}

module.exports.login = function(req,res){
    res.sendFile(path.join(__dirname,'../../login.html'));
 }

 module.exports.createUser = function(req,res){
    res.sendFile(path.join(__dirname,'../../createUser.html'));
 }

