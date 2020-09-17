

fs = require('fs');

var stdinBuffer = fs.readFileSync(0,'utf8');
var content = stdinBuffer.toString();
//console.log(content);

var convert = require('xml-js');
var options = {compact: true};
var result = convert.xml2json(content, options); // or convert.xml2json(xml, options)
console.log(result);

