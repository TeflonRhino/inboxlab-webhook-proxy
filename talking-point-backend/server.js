import express from 'express';
const app=express();
const port=Number(process.env.PORT||10000);
app.get('/health',(_req,res)=>res.json({ok:true,service:'talking-point-rtms'}));
app.listen(port,'0.0.0.0',()=>console.log('Talking Point backend listening on '+port));
