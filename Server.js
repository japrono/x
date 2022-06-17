require('dotenv').config();
const express = require('express');
const app = express();
const cors = require('cors');
app.use(cors(), express.json());
const port = 3001;
const pool = require('./dbpool');
const pool2 = require('./dbpool2');
const bcrypt = require('bcryptjs');
const nodemailer = require("nodemailer");
var cron = require('node-cron');
const ipify = require('ipify');
const fse = require('fs-extra');
const path = require('path');
const { exec } = require('child_process');
const {Builder, By, Key, until} = require('selenium-webdriver');
var firefox = require('selenium-webdriver/firefox');
const axios = require('axios');
var validator = require('validator');
const isOnline = require('is-online');
const cluster = require('cluster');
const os = require('os');
const fs = require('fs');
const PNG = require('pngjs').PNG;
const pixelmatch = require('pixelmatch');
var sizeOf = require('image-size');


const { createCanvas, loadImage } = require('canvas');
const myimg = loadImage('image.png');

async function main() {
  var image = await loadImage('image.png');
  var dimensions = sizeOf('./image.png');
  const canvas = createCanvas(dimensions.width, dimensions.height);
  const ctx = canvas.getContext('2d');
  ctx.drawImage(image, 0, 0, dimensions.width, dimensions.height);

  ctx.beginPath();
  ctx.rect(0, 0, 4667, 80);
  ctx.fillStyle = 'yellow';
  ctx.fill();


  fs.writeFileSync('output23.png', canvas.toBuffer())


}

if (require.main === module) {
  main();
}

loadImage('image.png').then((image) => {
  /*
     var dimensions = sizeOf('./image.png');
     const canvas = createCanvas(dimensions.width, dimensions.height);
     const ctx = canvas.getContext('2d');
     ctx.drawImage(image, 0, 0, dimensions.width, dimensions.height);

     ctx.beginPath();
     ctx.rect(0, 0, 4667, 80);
     ctx.fillStyle = 'yellow';
     ctx.fill();


  fs.writeFileSync('output.png', canvas.toBuffer())
  */
})



require("fs").readdirSync(require("path").join(__dirname, "routes")).forEach(function(file) {
  require("./routes/" + file)(app);
});

require("fs").readdirSync(require("path").join(__dirname, "crons")).forEach(function(file) {
  require("./crons/" + file)(app);
});



/*
function comparison(pic1, pic2, output) {
  let img1 = PNG.sync.read(fs.readFileSync(pic1));
  let img2 = PNG.sync.read(fs.readFileSync(pic2));
  let {width, height} = img1;
  let diff = new PNG({width, height});
  var numDiffPixels = pixelmatch(img1.data, img2.data, diff.data, width, height, {threshold: 0.1});
  fs.writeFileSync(output + `.png`, PNG.sync.write(diff));
}

comparison(`pic1.png`, `pic2.png`, `mydiff`);

comparison(`mcafee1.png`, `mcafee2.png`, `mcafee_diff`);

comparison(`same1.png`, `same2.png`, `same_diff`);

comparison(`mcafeeWebsite1.png`, `mcafeeWebsite2.png`, `mcafee_website_diff`);
*/



async function EmailExists(EmailAddress) {
  let val = true;

  try
  {
    const conn = await pool.getConnection();

    try
    {
      const [rows, fields] = await conn.query("SELECT EXISTS(SELECT * FROM Users WHERE EmailAddress = ?) AS Test", [EmailAddress]);
      val = rows[0].Test == 1 ? true : false;
    }
    catch(ex)
    {
      val = false;
    }
    finally
    {
        conn.release();
    }
  }
  catch(ex)
  {
    val = false;
  }
  finally
  {
      return val;
  }
}

async function GetHash(EmailAddress) {
  let val = "";

  try
  {
    const conn = await pool.getConnection();

    try
    {
      const [rows, fields] = await conn.query("SELECT Password AS Test FROM Users WHERE EmailAddress = ?", [EmailAddress]);
      val = rows[0].Test;
    }
    catch(ex)
    {
    }
    finally
    {
        conn.release();
    }
  }
  catch(ex)
  {
  }
  finally
  {
      return val;
  }
}

async function AreValidCredentials(EmailAddress, Password) {
  let val = false;
  var hash = await GetHash(EmailAddress);

  if(hash == "") {
    return false;
  }

  try
  {
    const match = await bcrypt.compare(Password, hash);

    if(match) {
      val = true;
    }
    else {
      val = false;
    }
  }
  catch(ex)
  {
  }
  finally
  {
      return val;
  }
}

async function GenerateToken(EmailAddress) {
  let Token = require('crypto').randomBytes(255).toString('hex').slice(0, 255);

  try {
    const conn = await pool.getConnection();

    try {
      const [rows, fields, err] = await conn.query("UPDATE Users SET Token = ? WHERE EmailAddress = ?", [Token, EmailAddress]);
    }catch(ex) {
      Token = "";
    } finally {
        conn.release();
    }
  }
  catch(ex) {
    Token="";
  }
  finally {
    return Token;
  }
}


app.post('/login', async function (req, res) {
   res.header("Access-Control-Allow-Origin", "*");
   res.header("Access-Control-Allow-Headers", "Origin", "X-Requested-With", "Content-Type", "Accept");

   let EmailAddress = req.body.EmailAddress;
   let Password = req.body.Password;

   if(!await EmailExists(EmailAddress)) {
     res.json({Response: "Failure", Message: "Account with the provided email address doesn't exist."});
   }
   else {
     var AVC = await AreValidCredentials(EmailAddress, Password);

     if(AVC) {
       let Token = await GenerateToken(EmailAddress);

       if(Token.length > 0) {
         res.json({Response: "Success", Token: Token});
       }
       else {
          res.json({Response: "Failure"});
       }
     }
     else {
       res.json({Response: "Failure", Message: "Password is incorrect."});
     }
   }
})



async function HashPassword(Password) {
  const hashedPassword = await bcrypt.hash(Password, 15);
  return hashedPassword;
}

async function CreateNewUser(EmailAddress, Password) {
  let Token = require('crypto').randomBytes(255).toString('hex').slice(0, 255);

  try {
    const conn = await pool.getConnection();
    Password = await HashPassword(Password);

    try {
      const [rows, fields, err] = await conn.query("INSERT INTO Users (EmailAddress, Password, Token) VALUES (?, ?, ?)", [EmailAddress, Password, Token]);
    }catch(ex) {
      Token = "";
    } finally {
        conn.release();
    }
  }
  catch(ex) {
    Token="";
  }
  finally {
    return Token;
  }
}

async function IsTokenValid(Token) {
  let val = false;

  try
  {
    const conn = await pool.getConnection();

    try
    {
      const [rows, fields] = await conn.query("SELECT EXISTS(SELECT * FROM Users WHERE Token = ?) AS Test", [Token]);
      val = rows[0].Test == 1 ? true : false;
    }
    catch(ex)
    {
    }
    finally
    {
        conn.release();
    }
  }
  catch(ex)
  {
  }
  finally
  {
      return val;
  }
}

async function SendResetPasswordEmail(EmailAddress, Token) {
  var success = true;

  try {
      let testAccount = await nodemailer.createTestAccount();

      let transporter = nodemailer.createTransport({
         host: "smtp.gmail.com",
         port: 465,
         secure: true,
         auth: {
           user: process.env.EMAIL_USERNAME,
           pass: process.env.EMAIL_PASSWORD
         }
      });

      let info = await transporter.sendMail({
        from: {name: "Saturn", address: process.env.EMAIL_USERNAME},
        to: EmailAddress,
        subject: "Saturn - Password reset link",
        text: process.env.CLIENT_PATH + "/forgot/reset/" + Token,
        html: "<b>Use this link to reset your password: <p> " + process.env.CLIENT_PATH + "/forgot/reset/" + Token + " </p> </b>",
      });
    }
  catch(ex) {
      success= false;
  }

  return success;
}

async function GeneratePasswordReset(EmailAddress) {
  var returnable = true;
  let Token = require('crypto').randomBytes(255).toString('hex').slice(0, 255);

  try {
    const conn = await pool.getConnection();

    try {
      const [rows, fields, err] = await conn.query("UPDATE Users SET PasswordReset = ?, PasswordResetGenerationDate = ? WHERE EmailAddress = ?", [Token, new Date().toISOString().slice(0, 19).replace('T', ' '), EmailAddress]);

      if(!err) {
        if(!await SendResetPasswordEmail(EmailAddress, Token)) {
          returnable = false;
        }

      }
      else {
        returnable = false;
      }

    }catch(ex) {
      returnable = false;
    } finally {
        conn.release();
    }
  }
  catch(ex) {
    returnable = false;
  }
  finally {
  }

  return returnable;
}

async function IsResetPasswordTokenValid(Token) {
  let Response = {Response: true, EmailAddress: ""};

  try
  {
    const conn = await pool.getConnection();

    try
    {
      const [rows, fields] = await conn.query("SELECT EmailAddress AS Test FROM Users WHERE PasswordReset = ? AND TIMESTAMPDIFF(SECOND, PasswordResetGenerationDate, NOW()) < 600", [Token]);
      val = rows[0].Test;
      Response.EmailAddress = val;
    }
    catch(ex)
    {
      Response.Response = false;
    }
    finally
    {
        conn.release();
    }
  }
  catch(ex)
  {
    Response.Response = false;
  }
  finally
  {
      return Response;
  }
}

async function ChangePassword(EmailAddress, Password) {
  var returnable = true;

  try {
    const conn = await pool.getConnection();
    Password = await HashPassword(Password);

    try {
      const [rows, fields, err] = await conn.query("UPDATE Users SET Password = ? WHERE EmailAddress = ?", [Password, EmailAddress]);
      if(err) {
        returnable = false;
      }
    }catch(ex) {
      returnable = false;
    } finally {
        conn.release();
    }
  }
  catch(ex) {
    returnable = false;
  }
  finally {
    return returnable;
  }
}

async function DeleteResetToken(Token) {
  try {
    const conn = await pool.getConnection();

    try {
      const [rows, fields, err] = await conn.query("UPDATE Users SET PasswordReset = NULL, PasswordResetGenerationDate = NULL WHERE PasswordReset = ?", [Token]);
    }catch(ex) {
    } finally {
        conn.release();
    }
  }
  catch(ex) {
  }
  finally {
  }
}

app.post('/ResetPassword', async function (req, res) {
   res.header("Access-Control-Allow-Origin", "*");
   res.header("Access-Control-Allow-Headers", "Origin", "X-Requested-With", "Content-Type", "Accept");

   let ResetPasswordToken = req.body.ResetPasswordToken;
   let Password = req.body.Password;

   var IRPTV = await IsResetPasswordTokenValid(ResetPasswordToken);

   if(IRPTV.Response) {
     let Token = await GenerateToken(IRPTV.EmailAddress);
     let PasswordChange = await ChangePassword(IRPTV.EmailAddress, Password);

     if(Token != "" && PasswordChange) {
        await DeleteResetToken(ResetPasswordToken);
        res.json({Response: "Success", EmailAddress: IRPTV.EmailAddress, Token: Token});
     }
     else {
       res.json({Response: "Failure"});
     }
   }
   else {
     res.json({Response: "Failure"});
   }
})

app.post('/IsResetPasswordTokenValid', async function (req, res) {
   res.header("Access-Control-Allow-Origin", "*");
   res.header("Access-Control-Allow-Headers", "Origin", "X-Requested-With", "Content-Type", "Accept");

   let ResetPasswordToken = req.body.ResetPasswordToken;

   var IRPTV = await IsResetPasswordTokenValid(ResetPasswordToken);

   if(IRPTV.Response) {
     res.json({Response: "Success", EmailAddress: IRPTV.EmailAddress});
   }
   else {
     res.json({Response: "Failure"});
   }
})

app.post('/retrievePassword', async function (req, res) {
   res.header("Access-Control-Allow-Origin", "*");
   res.header("Access-Control-Allow-Headers", "Origin", "X-Requested-With", "Content-Type", "Accept");

   let EmailAddress = req.body.EmailAddress;

   if(!await EmailExists(EmailAddress)) {
     res.json({Response: "Failure", Message: "Account with the provided email address doesn't exist."});
   }
   else {
     if(await GeneratePasswordReset(EmailAddress)) {
       res.json({Response: "Success"});
     }
     else {
      res.json({Response: "Failure"});
     }
   }
})

app.post('/IsTokenValid', async function (req, res) {
   res.header("Access-Control-Allow-Origin", "*");
   res.header("Access-Control-Allow-Headers", "Origin", "X-Requested-With", "Content-Type", "Accept");

   let Token = req.body.Token;

   if(await IsTokenValid(Token)) {
     res.json({Response: true});
   }
   else {
     res.json({Response: false});
   }
})


app.post('/register', async function (req, res) {
   res.header("Access-Control-Allow-Origin", "*");
   res.header("Access-Control-Allow-Headers", "Origin", "X-Requested-With", "Content-Type", "Accept");

   let EmailAddress = req.body.EmailAddress;
   let Password = req.body.Password;

   if(await EmailExists(EmailAddress)) {
     res.json({Response: "Failure", Message: "User already exists"});
   }
   else {
     let Token = await CreateNewUser(EmailAddress, Password);

     if(Token.length == 0) {
       res.json({Response: "Failure"});
     }
     else {
       res.json({Response: "Success", Token: Token});
     }
   }
})

async function getID(Token) {
  let Response = {Response: true, ID: -1};

  try
  {
    const conn = await pool.getConnection();

    try
    {
      const [rows, fields] = await conn.query("SELECT ID AS Test FROM Users WHERE Token = ?", [Token]);
      val = rows[0].Test;
      Response.ID = val;
    }
    catch(ex)
    {
      console.log(ex);
      Response.Response = false;
    }
    finally
    {
        conn.release();
    }
  }
  catch(ex)
  {
    Response.Response = false;
  }
  finally
  {
      return Response;
  }
}


async function CreateCrawl(AuthenticationToken, Name, StartingLinks) {
  var Response = {Response: true, ID: -1};

  try {
    const conn = await pool.getConnection();

    try {
      var UserID = await getID(AuthenticationToken);

      if(UserID.Response) {
        const [rows, fields, err] = await conn.query("INSERT INTO Crawls (Name, Status, CreatorID) VALUES (?, ?, ?)", [Name, "In Progress", UserID.ID]);
        if(err) {
          console.log(err);
          Response.Response = false;
        }
        else {

          var values = [

          ];

          StartingLinks.forEach((item, i) => {
            values.push( [rows.insertId, item.URL, 1] );
          });

          Response.ID = rows.insertId;

          const [rows2, fields2, err2] = await conn.query("INSERT INTO Links (CrawlID, Link, IS_STARTING_LINK) VALUES ?", [values]);

          if(err2) {
            Response.Response = false;
            console.log(err2);
          }
        }
      }
      else {
        Response.Response = false;
      }
    }
    catch(ex) {
      console.log(ex);
      Response.Response = false;
    }

    conn.release();
  }
  catch(ex) {
    console.log(ex);
    Response.Response = false;
  }
  finally {
    return Response;
  }
}


app.post('/CreateCrawl', async function (req, res) {
   res.header("Access-Control-Allow-Origin", "*");
   res.header("Access-Control-Allow-Headers", "Origin", "X-Requested-With", "Content-Type", "Accept");

   let AuthenticationToken = req.body.AuthenticationToken;
   let CrawlName = req.body.CrawlName;
   let StartingLinks = req.body.StartingLinks;

   if(await IsTokenValid(AuthenticationToken)) {
     var CC = await CreateCrawl(AuthenticationToken, CrawlName, StartingLinks);

     if(CC.Response) {
        res.json({Response: "Success", ID: CC.ID});
     }
     else {
       res.json({Response: "Failure"});
     }
   }
   else {
     res.json({Response: "Failure"});
   }
})

async function getPageCount() {
  let val = 1;

  try
  {
    const conn = await pool.getConnection();

    try
    {
      const [rows, fields] = await conn.query("SELECT CEILING(COUNT(*)/10) AS Test FROM Crawls;");
      val = rows[0].Test;
    }
    catch(ex)
    {
    }
    finally
    {
        conn.release();
    }
  }
  catch(ex)
  {
  }
  finally
  {
      return val;
  }
}

async function get_PAGE_COUNT(CrawlID, Filters) {
  let val = 1;

  try
  {
    const conn = await pool.getConnection();

    try
    {
      const [rows, fields] = await conn.query(
        "WITH Starting_Domains AS (select DISTINCT(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(Link, '/', 3), '://', -1), '/', 1), ?, 1)) AS domain from links where is_starting_link = 1 AND CrawlID = ? UNION SELECT domain FROM secondary_important_domains WHERE CrawlID = ?),"+
        "filtered_links AS ("+
        "Select * From links WHERE CrawlID=?"+
        "and (? is null or SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(Link, '/', 3), '://', -1), '/', 1), ?, 1) NOT IN (SELECT domain FROM Starting_Domains)  ) "+
        "and (? is null or SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(Link, '/', 3), '://', -1), '/', 1), ?, 1) IN (SELECT domain FROM Starting_Domains)  ) "+
        "and (? is null or (C1_Success = 1 OR C1_Success IS NULL AND C1_TryCount >=3) IS NULL) "+
        "and (? is null or (C1_Success = 1 OR C1_Success IS NULL AND C1_TryCount >=3) IS NOT NULL) "+
        "and (? is null or (VisitSuccess = 1 OR VisitSuccess IS NULL AND VisitTryCount >=3) IS NULL ) "+
        "and (? is null or (VisitSuccess = 1 OR VisitSuccess IS NULL AND VisitTryCount >=3) IS NOT NULL ) "+
        ") "+

        "SELECT CEILING(COUNT(*)/10) AS Test FROM filtered_links where CrawlID=? " +
      "AND IFNULL(ResponseCode, '') LIKE concat(\"%\", IFNULL(?, ''), \"%\") "+
      "AND IFNULL(Link, '') LIKE concat(\"%\", IFNULL(?, ''), \"%\") "+
      "AND IFNULL(ContentType, '') LIKE concat(\"%\", IFNULL(?, ''), \"%\") "+
      "AND IFNULL(Title, '') LIKE concat(\"%\", IFNULL(?, ''), \"%\") "+
      "AND IFNULL(Text, '') LIKE concat(\"%\", IFNULL(?, ''), \"%\") "
      , ["?", CrawlID,CrawlID, CrawlID, Filters.Importance_Important ? null : true, "?", Filters.Importance_Unimportant ? null : true, "?", Filters.Stage1_Complete ? null : true, Filters.Stage1_Remaining ? null : true, Filters.Stage2_Complete ? null : true, Filters.Stage2_Remaining ? null : true, CrawlID, Filters.ResponseCodeFilter, Filters.LinkFilter, Filters.ContentTypeFilter, Filters.TitleFilter, Filters.TextFilter]);

      val = rows[0].Test;
    }
    catch(ex)
    {
      console.log(ex);
    }
    finally
    {
        conn.release();
    }
  }
  catch(ex)
  {
  }
  finally
  {
      return val;
  }
}

async function getTotalCrawlingsCount() {
  let val = 0;

  try
  {
    const conn = await pool.getConnection();

    try
    {
      const [rows, fields] = await conn.query("SELECT COUNT(*) AS Test FROM Crawls;");
      val = rows[0].Test;
    }
    catch(ex)
    {
    }
    finally
    {
        conn.release();
    }
  }
  catch(ex)
  {
  }
  finally
  {
      return val;
  }
}

async function get_NOT_FULLY_COMPLETE_LINKS_COUNT(CrawlID) {
  let val = 0;

  try
  {
    const conn = await pool.getConnection();

    try
    {
      const [rows, fields, err] = await conn.query("SELECT COUNT(*) AS NOT_FULLY_COMPLETE_LINKS FROM Links where CrawlID=? AND C1_Success <> 1  OR VisitSuccess <> 1 OR C1_Success IS NULL AND C1_TryCount < 3 OR VisitSuccess IS NULL AND VisitTryCount < 3", [CrawlID]);
      val = rows[0].NOT_FULLY_COMPLETE_LINKS;

      if(err)
      {
        console.log(err);
      }

    }
    catch(ex)
    {
      console.log(ex);
    }
    finally
    {
        conn.release();
    }
  }
  catch(ex)
  {
  }
  finally
  {
      return val;
  }
}

async function get_FULLY_COMPLETE_LINKS_COUNT(CrawlID) {
  let val = 0;

  try
  {
    const conn = await pool.getConnection();

    try
    {
      const [rows, fields, err] = await conn.query("SELECT COUNT(*) AS FULLY_COMPLETE_LINKS FROM Links where CrawlID = ? AND C1_Success = 1 AND VisitSuccess = 1 OR VisitTryCount >= 3 OR C1_TryCount >= 3", [CrawlID]);
      val = rows[0].FULLY_COMPLETE_LINKS;

      if(err)
      {
        console.log(err);
      }

    }
    catch(ex)
    {
      console.log(ex);
    }
    finally
    {
        conn.release();
    }
  }
  catch(ex)
  {
  }
  finally
  {
      return val;
  }
}

async function get_FILTERED_LINKS_COUNT(CrawlID, Filters) {
  let val = 0;

  try
  {
    const conn = await pool.getConnection();

    try
    {
      const [rows, fields, err] = await conn.query(
        "WITH Starting_Domains AS (select DISTINCT(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(Link, '/', 3), '://', -1), '/', 1), ?, 1)) AS domain from links where is_starting_link = 1 AND CrawlID = ? UNION SELECT domain FROM secondary_important_domains WHERE CrawlID = ?),"+
        "filtered_links AS ("+
        "Select * From links WHERE CrawlID=?"+
        "and (? is null or SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(Link, '/', 3), '://', -1), '/', 1), ?, 1) NOT IN (SELECT domain FROM Starting_Domains)  ) "+
        "and (? is null or SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(Link, '/', 3), '://', -1), '/', 1), ?, 1) IN (SELECT domain FROM Starting_Domains)  ) "+
        "and (? is null or (C1_Success = 1 OR C1_Success IS NULL AND C1_TryCount >=3) IS NULL) "+
        "and (? is null or (C1_Success = 1 OR C1_Success IS NULL AND C1_TryCount >=3) IS NOT NULL) "+
        "and (? is null or (VisitSuccess = 1 OR VisitSuccess IS NULL AND VisitTryCount >=3) IS NULL ) "+
        "and (? is null or (VisitSuccess = 1 OR VisitSuccess IS NULL AND VisitTryCount >=3) IS NOT NULL ) "+
        ") "+

      "SELECT COUNT(*) AS ALL_LINKS_COUNT FROM filtered_links where CrawlID = ? " +
      "AND IFNULL(ResponseCode, '') LIKE concat(\"%\", IFNULL(?, ''), \"%\") "+
      "AND IFNULL(Link, '') LIKE concat(\"%\", IFNULL(?, ''), \"%\") "+
      "AND IFNULL(ContentType, '') LIKE concat(\"%\", IFNULL(?, ''), \"%\") "+
      "AND IFNULL(Title, '') LIKE concat(\"%\", IFNULL(?, ''), \"%\") "+
      "AND IFNULL(Text, '') LIKE concat(\"%\", IFNULL(?, ''), \"%\") ",

      ["?", CrawlID,CrawlID, CrawlID, Filters.Importance_Important ? null : true, "?", Filters.Importance_Unimportant ? null : true, "?", Filters.Stage1_Complete ? null : true, Filters.Stage1_Remaining ? null : true, Filters.Stage2_Complete ? null : true, Filters.Stage2_Remaining ? null : true, CrawlID, Filters.ResponseCodeFilter, Filters.LinkFilter, Filters.ContentTypeFilter, Filters.TitleFilter, Filters.TextFilter]);

      val = rows[0].ALL_LINKS_COUNT;

      if(err)
      {
        console.log(err);
      }

    }
    catch(ex)
    {
      console.log(ex);
    }
    finally
    {
        conn.release();
    }
  }
  catch(ex)
  {
  }
  finally
  {
      return val;
  }
}

async function get_ALL_LINKS_COUNT(CrawlID) {
  let val = 0;

  try
  {
    const conn = await pool.getConnection();

    try
    {
      const [rows, fields, err] = await conn.query("SELECT COUNT(*) AS ALL_LINKS_COUNT FROM Links where CrawlID = ?", [CrawlID]);
      val = rows[0].ALL_LINKS_COUNT;

      if(err)
      {
        console.log(err);
      }

    }
    catch(ex)
    {
      console.log(ex);
    }
    finally
    {
        conn.release();
    }
  }
  catch(ex)
  {
  }
  finally
  {
      return val;
  }
}

async function GetCrawlings(Page, Token) {
  let Response = { Response: true, Crawlings: [], PageCount: await getPageCount(), TotalCrawlings: await getTotalCrawlingsCount() };

  try
  {
    const conn = await pool.getConnection();

    try
    {
      var Offset1 = 0;
      var Offset2 = 0;

      if(Page == 1) {
        Offset1 = 0;
        Offset2 = 10;
      }
      else {
        Offset1 = (Page * 10) - 10;
        Offset2 = Page * 10;
      }

        const [rows, fields, err] = await conn.query("SELECT ID, Name, Status, (SELECT EmailAddress FROM Users WHERE ID = CreatorID) AS Creator FROM Crawls c LIMIT ?, ?", [Offset1, Offset2]);

        if(!err) {
          var resultArray = Object.values(JSON.parse(JSON.stringify(rows)));
          Response.Crawlings = resultArray;
        }
        else {
          console.log(err);
          Response.Response = false;
        }
    }
    catch(ex)
    {
      console.log(ex);
      Response.Response = false;
    }
    finally
    {
        conn.release();
    }
  }
  catch(ex)
  {
    Response.Response = false;
  }
  finally
  {
      return Response;
  }
}

async function GetCrawlDetails(CrawlID) {
  let val = { TotalStartingLinks: 0, Response: false, StartingLinks: ["test"], CrawlName: "", Status: "", LinksCrawled: 0, LinksLeftToCrawl: 0, LinksUnableToCrawl: 0 };

  try
  {
    const conn = await pool.getConnection();

    try
    {
      const [rows, fields, err] = await conn.query("SELECT Name AS Test, Status AS Test1, (SELECT COUNT(*) FROM links WHERE CrawlID=? AND VisitSuccess = 1) AS LinksCrawled, (SELECT COUNT(*) FROM links WHERE VisitTryCount >= 3 AND CrawlID=?) AS LinksUnableToCrawl, (SELECT COUNT(*) FROM Links WHERE (VisitTryCount < 3 OR VisitTryCount IS NULL) AND CrawlID=? AND VisitSuccess IS NULL) AS LinksLeftToCrawl FROM Crawls WHERE ID = ?", [CrawlID, CrawlID, CrawlID, CrawlID]);

      if(!err) {
        val.CrawlName = rows[0].Test;
        val.Status = rows[0].Test1;
        val.LinksCrawled = rows[0].LinksCrawled;
        val.LinksLeftToCrawl = rows[0].LinksLeftToCrawl;
        val.LinksUnableToCrawl = rows[0].LinksUnableToCrawl;

        const [rows2, fields2, err2] = await conn.query("SELECT Link FROM Links WHERE CrawlID = ? AND IS_STARTING_LINK = 1", [CrawlID]);

        if(!err2) {
          var resultArray = Object.values(JSON.parse(JSON.stringify(rows2)));
          val.StartingLinks = resultArray;
          val.Response = true;
        }
      }
    }
    catch(ex)
    {

    }
    finally
    {
        conn.release();
    }
  }
  catch(ex)
  {
  }
  finally
  {
      return val;
  }
}

app.post('/GetCrawlDetails', async function (req, res) {
   res.header("Access-Control-Allow-Origin", "*");
   res.header("Access-Control-Allow-Headers", "Origin", "X-Requested-With", "Content-Type", "Accept");

   let AuthenticationToken = req.body.AuthenticationToken;
   let CrawlID = req.body.CrawlID;

   if(await IsTokenValid(AuthenticationToken)) {
     var CrawlDetails = await GetCrawlDetails(CrawlID);

     if(CrawlDetails.Response) {
        res.json({Response: "Success", CrawlDetails: CrawlDetails});
     }
     else {
       res.json({Response: "Failure"});
     }
   }
   else {
     res.json({Response: "Failure"});
   }
})

async function GetLinksFull(Page, CrawlID, Filters, Sort) {
  let Response = {
    Response: true,
    Links: [],
    PAGE_COUNT: await get_PAGE_COUNT(CrawlID, Filters),
    ALL_LINKS_COUNT: await get_ALL_LINKS_COUNT(CrawlID),
    FULLY_COMPLETE_LINKS_COUNT: await get_FULLY_COMPLETE_LINKS_COUNT(CrawlID),
    NOT_FULLY_COMPLETE_LINKS_COUNT: await get_NOT_FULLY_COMPLETE_LINKS_COUNT(CrawlID),
    FILTERED_LINKS_COUNT: await get_FILTERED_LINKS_COUNT(CrawlID, Filters)
  };

  try
  {
    const conn = await pool.getConnection();

    try
    {
        var Offset2 = Page * 10 - 10;

        const sql = conn.format(
          "WITH Starting_Domains AS (select DISTINCT(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(Link, '/', 3), '://', -1), '/', 1), ?, 1)) AS domain from links where is_starting_link = 1 AND CrawlID = ? UNION SELECT domain FROM secondary_important_domains WHERE CrawlID = ?),"+
          "filtered_links AS ("+
          "Select * From links WHERE CrawlID=?"+
          "and (? is null or SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(Link, '/', 3), '://', -1), '/', 1), ?, 1) NOT IN (SELECT domain FROM Starting_Domains)  ) "+
          "and (? is null or SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(Link, '/', 3), '://', -1), '/', 1), ?, 1) IN (SELECT domain FROM Starting_Domains)  ) "+
          "and (? is null or (C1_Success = 1 OR C1_Success IS NULL AND C1_TryCount >=3) IS NULL) "+
          "and (? is null or (C1_Success = 1 OR C1_Success IS NULL AND C1_TryCount >=3) IS NOT NULL) "+
          "and (? is null or (VisitSuccess = 1 OR VisitSuccess IS NULL AND VisitTryCount >=3) IS NULL ) "+
          "and (? is null or (VisitSuccess = 1 OR VisitSuccess IS NULL AND VisitTryCount >=3) IS NOT NULL ) "+
          ") "+
        "SELECT ID, Link, " +
        "(SELECT IF(C1_Success = 1 OR C1_Success IS NULL AND C1_TryCount >=3, \"YES\", \"NO\")) AS Stage1_Complete, "+
        "DATE_FORMAT(C1_SuccessDate, \"%d %M %Y, %H:%i:%S\") AS Stage1_Timestamp, ResponseCode, ContentType, "+
        "(SELECT IF(VisitSuccess = 1 OR VisitSuccess IS NULL AND VisitTryCount >=3, \"YES\",\"NO\")) AS Stage2_Complete, "+
        "DATE_FORMAT(DateTimeVisited, \"%d %M %Y, %H:%i:%S\") AS Stage2_Timestamp, "+
        "Title, "+
        "Text, "+
        "Links_COUNT AS LinkCount, " +
        "Pathways_COUNT AS Pathways " +
        "FROM filtered_links l "+
        "where "+
        "CrawlID=? "+
        "AND IFNULL(ResponseCode, '') LIKE concat(\"%\", IFNULL(?, ''), \"%\") "+
        "AND IFNULL(Link, '') LIKE concat(\"%\", IFNULL(?, ''), \"%\") "+
        "AND IFNULL(ContentType, '') LIKE concat(\"%\", IFNULL(?, ''), \"%\") "+
        "AND IFNULL(Title, '') LIKE concat(\"%\", IFNULL(?, ''), \"%\") "+
        "AND IFNULL(Text, '') LIKE concat(\"%\", IFNULL(?, ''), \"%\") "+
        "ORDER BY " +
       "CASE WHEN ? THEN Link END DESC,"+
       "CASE WHEN ? THEN Link END ASC,"+
       "CASE WHEN ? THEN ContentType END DESC,"+
       "CASE WHEN ? THEN ContentType END ASC,"+
  	   "CASE WHEN ? THEN Title END DESC,"+
       "CASE WHEN ? THEN Title END ASC,"+
  	   "CASE WHEN ? THEN Text END DESC,"+
       "CASE WHEN ? THEN Text END ASC,"+
       "CASE WHEN ? THEN cast(Links_COUNT as unsigned) END DESC,"+
       "CASE WHEN ? THEN cast(Links_COUNT as unsigned) END ASC,"+
  	   "CASE WHEN ? THEN cast(Pathways_COUNT as unsigned) END DESC,"+
       "CASE WHEN ? THEN cast(Pathways_COUNT as unsigned) END ASC,"+
  	   "CASE WHEN ? THEN cast(ResponseCode as signed) END DESC,"+
       "CASE WHEN ? THEN cast(ResponseCode as signed) END ASC,"+
       "CASE WHEN ? THEN C1_SuccessDate END DESC,"+
  	   "CASE WHEN ? THEN C1_SuccessDate END ASC, "+
       "CASE WHEN ? THEN DateTimeVisited END DESC,"+
      "CASE WHEN ? THEN DateTimeVisited END ASC, "+
       "CASE WHEN ? THEN ID END ASC "+
       "LIMIT 10 OFFSET ?", ["?", CrawlID,CrawlID, CrawlID, Filters.Importance_Important ? null : true, "?", Filters.Importance_Unimportant ? null : true, "?", Filters.Stage1_Complete ? null : true, Filters.Stage1_Remaining ? null : true, Filters.Stage2_Complete ? null : true, Filters.Stage2_Remaining ? null : true, CrawlID, Filters.ResponseCodeFilter, Filters.LinkFilter, Filters.ContentTypeFilter, Filters.TitleFilter, Filters.TextFilter, (Sort.SortOrder == "Link" && Sort.SortType == "DESC"), (Sort.SortOrder == "Link" && Sort.SortType == "ASC"), (Sort.SortOrder == "Content Type" && Sort.SortType == "DESC"), (Sort.SortOrder == "Content Type" && Sort.SortType == "ASC"), (Sort.SortOrder == "Title" && Sort.SortType == "DESC"), (Sort.SortOrder == "Title" && Sort.SortType == "ASC"), (Sort.SortOrder == "Text" && Sort.SortType == "DESC"), (Sort.SortOrder == "Text" && Sort.SortType == "ASC"), (Sort.SortOrder == "Links" && Sort.SortType == "DESC"), (Sort.SortOrder == "Links" && Sort.SortType == "ASC"), (Sort.SortOrder == "Pathways" && Sort.SortType == "DESC"), (Sort.SortOrder == "Pathways" && Sort.SortType == "ASC"), (Sort.SortOrder == "Response Code" && Sort.SortType == "DESC"), (Sort.SortOrder == "Response Code" && Sort.SortType == "ASC"), (Sort.SortOrder == "Stage1" && Sort.SortType == "DESC"), (Sort.SortOrder == "Stage1" && Sort.SortType == "ASC"), (Sort.SortOrder == "Stage2" && Sort.SortType == "DESC"), (Sort.SortOrder == "Stage2" && Sort.SortType == "ASC"), !Sort.SortOrder, Offset2]);

        const [rows, fields, err] = await conn.query(sql);

        if(!err) {
          var resultArray = Object.values(JSON.parse(JSON.stringify(rows)));
          Response.Links = resultArray;
        }
        else {
          Response.Response = false;
        }
    }
    catch(ex)
    {
      console.log(ex);
      Response.Response = false;
    }
    finally
    {
        conn.release();
    }
  }
  catch(ex)
  {
    console.log(ex);
    Response.Response = false;
  }
  finally
  {
      return Response;
  }
}

app.post('/GetLinksFull', async function (req, res) {
   res.header("Access-Control-Allow-Origin", "*");
   res.header("Access-Control-Allow-Headers", "Origin", "X-Requested-With", "Content-Type", "Accept");

   let AuthenticationToken = req.body.AuthenticationToken;
   let Page = req.body.Page;
   let CrawlID = req.body.CrawlID;
   let Filters = req.body.Filters;
   let Sort = req.body.Sort;

   if(await IsTokenValid(AuthenticationToken)) {
     var Links = await GetLinksFull(Page, CrawlID, Filters, Sort);

     if(Links.Response) {
        res.json({Response: "Success", Links: Links.Links, PAGE_COUNT: Links.PAGE_COUNT, ALL_LINKS_COUNT: Links.ALL_LINKS_COUNT, FULLY_COMPLETE_LINKS_COUNT: Links.FULLY_COMPLETE_LINKS_COUNT, NOT_FULLY_COMPLETE_LINKS_COUNT: Links.NOT_FULLY_COMPLETE_LINKS_COUNT, FILTERED_LINKS_COUNT: Links.FILTERED_LINKS_COUNT  });
     }
     else {
       res.json({Response: "Failure"});
     }
   }
   else {
     res.json({Response: "Failure"});
   }
})

app.post('/GetCrawlingsList', async function (req, res) {
   res.header("Access-Control-Allow-Origin", "*");
   res.header("Access-Control-Allow-Headers", "Origin", "X-Requested-With", "Content-Type", "Accept");

   let AuthenticationToken = req.body.AuthenticationToken;
   let Page = req.body.Page;

   if(await IsTokenValid(AuthenticationToken)) {
     var Crawlings = await GetCrawlings(Page, AuthenticationToken);

     if(Crawlings.Response) {
        res.json({Response: "Success", Crawlings: Crawlings.Crawlings, PageCount: Crawlings.PageCount, TotalCrawlings: Crawlings.TotalCrawlings});
     }
     else {
       res.json({Response: "Failure"});
     }
   }
   else {
     res.json({Response: "Failure"});
   }
})



async function ExecuteSQLInsert(SQL) {
  try {
    const conn = await pool.getConnection();

    try {
      const [rows, fields, err] = await conn.query(SQL);
      if(err) {
      }
    }catch(ex) {
    } finally {
        conn.release();
    }
  }
  catch(ex) {
  }
  finally {
  }
}

async function CreateDatabase(SQL) {
  try {
    const conn = await pool2.getConnection();

    try {
      const [rows, fields, err] = await conn.query(SQL);
      if(err) {
      }
    }catch(ex) {
    } finally {
        conn.release();
    }
  }
  catch(ex) {
  }
  finally {
  }
}

var cronTaskRunning = false;

async function ResetPasswordCleanup() {
  try {
    if (cronTaskRunning) {
      return;
    }

    cronTaskRunning = true;

    const conn = await pool.getConnection();

      try {
      const [rows, fields, err] = await conn.query("SET SQL_SAFE_UPDATES=0;");
      if(err) {
      }
      }catch(ex) {
      }

      try {
      const [rows, fields, err] = await conn.query("UPDATE Users SET PasswordResetGenerationDate = NULL, PasswordReset=NULL WHERE TIMESTAMPDIFF(SECOND, PasswordResetGenerationDate, NOW()) > 600;");
      if(err) {
      }
      }catch(ex) {
      }

        try {
        const [rows, fields, err] = await conn.query("SET SQL_SAFE_UPDATES=1;");
        if(err) {
        }
        }catch(ex) {
        } finally {
            conn.release();
        }
  }
  catch(ex) {
  }
  finally {
    cronTaskRunning = false;
  }
}

var CronInactiveCrawlSessionsActive = false;

async function InactiveCrawlSessionsCleanup() {
  try {
    if (CronInactiveCrawlSessionsActive) {
      return;
    }

    CronInactiveCrawlSessionsActive = true;

    const conn = await pool.getConnection();

      try {
      const [rows, fields, err] = await conn.query("SET SQL_SAFE_UPDATES=0;");
      if(err) {
      }
      }catch(ex) {
      }

      try {
      const [rows, fields, err] = await conn.query("UPDATE Links SET Crawl_Session_Id = NULL, CrawlSince=NULL, Crawl_In_Progress=0 WHERE TIMESTAMPDIFF(SECOND, CrawlSince, NOW()) > 600;");
      if(err) {
      }

      }catch(ex) {
      }

        try {
        const [rows, fields, err] = await conn.query("SET SQL_SAFE_UPDATES=1;");
        if(err) {
        }
        }catch(ex) {
        } finally {
            conn.release();
        }
  }
  catch(ex) {
  }
  finally {
    CronInactiveCrawlSessionsActive = false;
  }
}

async function GetLocalhostInformation() {
  let val = {Status: 0, MaximumSessions: 0};

  try
  {
    const conn = await pool.getConnection();

    try
    {
      const [rows, fields] = await conn.query("SELECT MaximumSessions, Status FROM Agents WHERE Name=?", ["localhost"]);
      val = rows[0];
    }
    catch(ex)
    {
    }
    finally
    {
        conn.release();
    }
  }
  catch(ex)
  {
  }
  finally
  {
      return val;
  }
}


async function Setup() {
  // reset db variables
//  await Set_Controller2_Running(false);
  //await Set_Controller1_Running(false);
//  await Set_Controller_Blocked(false);
  //


// start selenium grid
/*
try {
  var lhi = await GetLocalhostInformation();
  var status = lhi.Status;

  if(status) {
    var commandToExecute = "call " + path.join(__dirname, "closeCommandLines.bat");
    exec(commandToExecute);
    await sleep(5000);
    var commandToExecute2 = "start cmd /c " + path.join(__dirname, "selenium_grid.bat " + lhi.MaximumSessions);
    exec(commandToExecute2);
  }
  else {
    var commandToExecute = "call " + path.join(__dirname, "closeCommandLines.bat");
    exec(commandToExecute);
  }

} catch (e) {
  console.log(e);
} finally {

}*/
//

  // create database
  try {
    var CreateDatabaseStatement = "CREATE DATABASE IF NOT EXISTS `saturn` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;";
    var CreateUserTableStatement = "CREATE TABLE IF NOT EXISTS `users` (`ID` int NOT NULL AUTO_INCREMENT, `EmailAddress` varchar(255) NOT NULL, `Password` varchar(255) NOT NULL, `Token` varchar(255) DEFAULT NULL, `PasswordReset` varchar(255) DEFAULT NULL, `PasswordResetGenerationDate` datetime DEFAULT NULL, PRIMARY KEY (`ID`), UNIQUE KEY `EmailAddress_UNIQUE` (`EmailAddress`), UNIQUE KEY `Token_UNIQUE` (`Token`), UNIQUE KEY `PasswordReset_UNIQUE` (`PasswordReset`) ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;";

    await CreateDatabase(CreateDatabaseStatement);
    await ExecuteSQLInsert(CreateUserTableStatement);
  }
  catch(ex) {
  }
  //

  try {
    //cron.schedule('*/10 * * * *', () => { ResetPasswordCleanup(); });
  //  c/ron.schedule('*/60 * * * * *', () => { Controller1(); });
    //cron.schedule('*/60 * * * * *', () => { Controller2(); });
    //cron.schedule('*/15 * * * *', () => { InactiveCrawlSessionsCleanup(); });


    //cron.schedule('*/60 * * * * *', () => { Reformatter(); });
  }
  catch(ex) {
  }
}

function sleep(ms) {
   return new Promise((resolve) => {
      setTimeout(resolve, ms);
   });
}


async function GetLinksToCrawl() {
  let Links = [];

  try
  {
    const conn = await pool.getConnection();

    try
    {
      const [rows, fields, err] = await conn.query("WITH Starting_Domains AS (select crawlid, SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(Link, '/', 3), '://', -1), '/', 1), '?', 1) AS domain from links where is_starting_link = 1 GROUP BY CrawlID, domain), Visitable_Links AS (SELECT ID, Link, CrawlID, VisitTryCount, SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(Link, '/', 3), '://', -1), '/', 1), '?', 1) AS domain, (SELECT domain IN (SELECT domain FROM Starting_Domains WHERE CrawlID = l.CrawlID) ) AS IS_DOMAIN_SAME FROM Links l WHERE (VisitTryCount < 3 OR VisitTryCount IS NULL) AND (TIMESTAMPDIFF(SECOND, LastVisitTry, NOW()) > 600 OR LastVisitTry IS NULL) AND VisitSuccess IS NULL AND CrawlID IN (SELECT ID FROM Crawls WHERE Status = 'In Progress') AND Crawl_In_Progress = 0) SELECT ID, Link, CrawlID, VisitTryCount FROM Visitable_Links ORDER BY IS_DOMAIN_SAME DESC LIMIT 5;");
      if(!err) {
        var resultArray = Object.values(JSON.parse(JSON.stringify(rows)));
        Links = resultArray;
      }
    }
    catch(ex)
    {
    }
    finally
    {
        conn.release();
    }
  }
  catch(ex)
  {
  }
  finally
  {
      return Links;
  }
}

async function IncrementVisitTryCount(LinkID, DateTimeTry) {
  var returnable = true;

  try {
    const conn = await pool.getConnection();

    try {
      const [rows, fields, err] = await conn.query("UPDATE Links SET VisitTryCount = VisitTryCount + 1, LastVisitTry=? WHERE id = ?", [DateTimeTry, LinkID]);
      if(err) {
        returnable = false;
        console.log(err);
      }
    }catch(ex) {
      returnable = false;
      console.log(ex);
    } finally {
        conn.release();
    }
  }
  catch(ex) {
    console.log(ex);
    returnable = false;
  }
  finally {
    return returnable;
  }
}

async function UpdateLinkVisited(LinkID, ProcessedLinkData) {
  var returnable = true;

  try {
    const conn = await pool.getConnection();


    try {
      const [rows, fields, err] = await conn.query("UPDATE Links SET DateTimeVisited = ?, VisitSuccess = ?, Exception = ?, Title = ?, Text = ?, ResponseCode = ? WHERE id = ?", [ProcessedLinkData.Date, ProcessedLinkData.VisitSuccess ? true : null, ProcessedLinkData.Exception, ProcessedLinkData.Title, ProcessedLinkData.Text, ProcessedLinkData.ResponseCode, LinkID]);
      if(err) {
        returnable = false;
        console.log(err);
      }
    }catch(ex) {
      returnable = false;
      console.log(ex);
    } finally {
        conn.release();
    }
  }
  catch(ex) {
    console.log(ex);
    returnable = false;
  }
  finally {
    return returnable;
  }
}

async function LinkExistsInCrawl(Link, CrawlID) {
  let val = false;

  try
  {
    const conn = await pool.getConnection();

    try
    {
      const [rows, fields] = await conn.query("SELECT EXISTS(SELECT * FROM Links WHERE Link = ? AND CrawlID = ?) AS Test", [Link, CrawlID]);
      val = rows[0].Test == 1 ? true : false;
    }
    catch(ex)
    {
      console.log(ex);
    }
    finally
    {
        conn.release();
    }
  }
  catch(ex)
  {
    console.log(ex);
  }
  finally
  {
      return val;
  }
}

async function InsertLinkIntoCrawl(Link, CrawlID) {
  var returnable = null;

  try {
    const conn = await pool.getConnection();

    try {
      const [rows, fields, err] = await conn.query("INSERT INTO Links (Link, CrawlID) VALUES (?, ?)", [Link, CrawlID]);
      if(!err) {
        returnable = rows.insertId;
      }
    }catch(ex) {
      console.log(ex);

    } finally {
        conn.release();
    }
  }
  catch(ex) {
    console.log(ex);

  }
  finally {
    return returnable;
  }
}

async function GetLinkID(CrawlID, Link) {
  let val = null;

  try
  {
    const conn = await pool.getConnection();

    try
    {
      const [rows, fields] = await conn.query("SELECT ID AS Test FROM Links WHERE CrawlID=? AND Link=?", [CrawlID, Link]);
      val = rows[0].Test;
    }
    catch(ex)
    {
      console.log(ex);
    }
    finally
    {
        conn.release();
    }
  }
  catch(ex)
  {
    console.log(ex);
  }
  finally
  {
      return val;
  }
}

async function InsertRelationship(ParentLinkID, LinkID, TextHref) {
  var returnable = null;

  try {
    const conn = await pool.getConnection();

    try {
      const [rows, fields, err] = await conn.query("INSERT INTO LinkRelationships (ParentID, LinkID, TextHref) VALUES (?, ?, ?)", [ParentLinkID, LinkID, TextHref]);
    }catch(ex) {
      console.log(ex);
    } finally {
        conn.release();
    }
  }
  catch(ex) {
    console.log(ex);
  }
  finally {
    return returnable;
  }
}

async function InsertLinksToLinks(LinkID, Links, CrawlID) {
  try {
    for (const link of Links) {
      try {
            if(link.href && validator.isURL(link.href, {require_protocol: true, protocols: ["https", "http"], require_tld: false    }   )) {
                var existsin = await LinkExistsInCrawl(link.href, CrawlID);
                var newLinkID = null;

                if(!existsin) {
                    newLinkID = await InsertLinkIntoCrawl(link.href, CrawlID);
                }
                else {
                  newLinkID = await GetLinkID(CrawlID, link.href);
                }

                if(newLinkID) {
                  await InsertRelationship(LinkID, newLinkID, link.text);
                }
            }
      }
      catch(ex){
        console.log(ex);
      }
    }

  }catch(ex){
    console.log(ex);
  }
}

async function UpdateProcessedLink(ProcessedLink, ProcessedLinkData) {
  try {
    if(!ProcessedLinkData.VisitSuccess) {
        await IncrementVisitTryCount(ProcessedLink.ID, ProcessedLinkData.Date);
    }

    await UpdateLinkVisited(ProcessedLink.ID, ProcessedLinkData);
    await InsertLinksToLinks(ProcessedLink.ID, ProcessedLinkData.Links, ProcessedLink.CrawlID);

  }catch(ex) {

  }
}

async function IsSeleniumGridAvailable() {
  var returnable = false;

  try {
    const responseQueue = await axios.get("http://localhost:4444/se/grid/newsessionqueue/queue");
    var count = responseQueue.data.value.length;

    if(count == 0) {
      const response = await axios.get("http://localhost:4444/status");


      var test = response.data.value.nodes[0].slots;

      for (const slot of test) {
        if(slot.session == null) {
          return true;
        }
      }
    }
  }
  catch(ex) {
  }

  return returnable;
}

async function RegisterSession(LinkID, SessionID) {
  try {
    const conn = await pool.getConnection();

    try {
      const [rows, fields, err] = await conn.query("UPDATE Links SET Crawl_In_Progress = 1, CrawlSince=?, Crawl_Session_Id = ? WHERE ID = ?", [new Date().toISOString().slice(0, 19).replace('T', ' '), SessionID, LinkID]);

      if(err) {
        console.log(err);
      }
    }
    catch(ex) {
      console.log(ex);
    }
    finally {
        conn.release();
    }
  }
  catch(ex) {
    console.log(ex);
  }
  finally {
  }
}

async function Cotroller2_Processor(Link) {
  try {
    var ID = Link.ID;
    delete Link.ID;
    var Link = Object.assign(Link, {Crawl_Session_Id: null, Crawl_In_Progress: 0, CrawlSince: null, VisitTryCount: Link.VisitTryCount + 1, Exception: null, Title: null, Text: null, Screenshot: null});
    var ChildLinks = [];

    try {
      var options = new firefox.Options();
      options.addArguments("--headless");

      var driver = await new Builder().forBrowser('firefox').usingServer('http://localhost:4444/wd/hub').setFirefoxOptions(options).build();
      var sessionId = await driver.getSession();
      await RegisterSession(ID, sessionId.id_);

      await driver.manage().setTimeouts({pageLoad: 60000, implicit: 0, script: 10000});
      await driver.get(Link.Link);
      await sleep(10000);

      try {
        Link.Text = await driver.findElement(By.tagName("body")).getText();
      } catch (e) {
      } finally {
      }

      try {
        Link.Title = await driver.getTitle();
      } catch (e) {
      } finally {
      }

      var elems = await driver.findElements(By.tagName('a'));

      for (const link of elems) {
        try {
            var ChildLink = {text: null, href: null};

            try {
              ChildLink.text = await link.getAttribute("innerText");
            } catch (e) {

            } finally {

            }

            try {
              ChildLink.href = await link.getAttribute("href");
            } catch (e) {

            } finally {

            }

            ChildLinks.push( ChildLink );

        }
        catch(ex) {
          //console.log(ex);
        }
      }

      Link.VisitSuccess = true;
      Link.DateTimeVisited = new Date().toISOString().slice(0, 19).replace('T', ' ');
    } catch (e) {
      Link = Object.assign(Link, {Exception: e.stack, LastVisitTry: new Date().toISOString().slice(0, 19).replace('T', ' ')});
    }

    finally {
                try {
                  driver.close();
                } catch (e) {

                } finally {

                }
                try {
                  driver.quit();
                } catch (e) {

                } finally {

                }

                try {
                  // TODO quit sessionid from rest api
                } catch (e) {

                } finally {

                }
    }
  }
  catch(ex) {
    Link = Object.assign(Link, {Exception: ex.stack, LastVisitTry: new Date().toISOString().slice(0, 19).replace('T', ' ')});
  }
  finally {
    if(!Link.Exception) {
      await UpdateLink(Link, ID);
      await InsertLinksToLinks(ID, ChildLinks, Link.CrawlID);
    }
    else
    {
            if(await isOnline()) {
              await UpdateLink(Link, ID);
              await InsertLinksToLinks(ID, ChildLinks, Link.CrawlID);
            }
            else {
              return "offline";
            }
    }
  }
}

async function Controller2() {
  try {
    if (await Get_Controller2_Running() || await Get_Controller_Blocked()) {
      return;
    }

    await Set_Controller2_Running(true);
    var NoLinksLeft = false;
    var InternetWorking = await isOnline();

    while(!NoLinksLeft && !await Get_Controller_Blocked() && InternetWorking) {
      var Links = await GetLinksToCrawl();

      if(Links.length == 0) {
        NoLinksLeft = true;
      }
      else {
        for await (const link of Links) {
          if(await Get_Controller_Blocked()) {
            var commandToExecute = "call " + path.join(__dirname, "closeCommandLines.bat");
            exec(commandToExecute);
            break;
          }

          var IsCrawlInProgressVar = await IsCrawlInProgress(link.CrawlID);

          if(!IsCrawlInProgressVar || await Get_Controller_Blocked()) {
            continue;
          }

          /* todo: since we're changing functionality, it will be controlled in dashboard  so we don't turn it on for no reason only if enabled
          if(!await IsSeleniumGridAvailable())
          {
            var s = await IsSeleniumGridOnline() ;
            if(!s) {
              try {
                var commandToExecute = "call " + path.join(__dirname, "closeCommandLines.bat");
                exec(commandToExecute);
                await sleep(5000);
                var commandToExecute2 = "start cmd /c " + path.join(__dirname, "selenium_grid.bat");
                exec(commandToExecute2);
                await sleep(15000);
              } catch (e) {
                console.log(e);
              } finally {
              }
            }
          }*/

          if(await IsSeleniumGridAvailable() && !await Get_Controller_Blocked()) {
            var s = Cotroller2_Processor(link);

            if(s === "offline") {
              InternetWorking = false;
              break;
            }

          }



          await sleep(1000);
        }



      }

      await sleep(2500);
    }

    if(!InternetWorking) {
      await sleep(60000);
    }

    await Set_Controller2_Running(false);
  }
  catch(ex) {
    console.log(ex);
    await Set_Controller2_Running(false);
  }
  finally {
  }
}

async function GetEmailAddress(AuthenticationToken) {
  let val = "";

  try
  {
    const conn = await pool.getConnection();

    try
    {
      const [rows, fields] = await conn.query("SELECT EmailAddress AS Test FROM Users WHERE Token = ?", [AuthenticationToken]);
      val = rows[0].Test;
    }
    catch(ex)
    {
    }
    finally
    {
        conn.release();
    }
  }
  catch(ex)
  {
  }
  finally
  {
      return val;
  }
}

app.post('/GetEmailAddress', async function (req, res) {
   res.header("Access-Control-Allow-Origin", "*");
   res.header("Access-Control-Allow-Headers", "Origin", "X-Requested-With", "Content-Type", "Accept");

   let AuthenticationToken = req.body.AuthenticationToken;

   if(await IsTokenValid(AuthenticationToken)) {
     var EmailAddress = await GetEmailAddress(AuthenticationToken);

     if(EmailAddress != "") {
        res.json({Response: "Success", EmailAddress: EmailAddress});
     }
     else {
       res.json({Response: "Failure"});
     }
   }
   else {
     res.json({Response: "Failure"});
   }
})










async function GetLinksToController1() {
  let Links = [];

  try
  {
    const conn = await pool.getConnection();


    try
    {
      const [rows, fields, err] = await conn.query("WITH Crawls_In_progress AS (SELECT ID FROM Crawls WHERE Status = 'In Progress'), Starting_Domains AS (SELECT CrawlID, SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(Link, '/', 3), '://', -1), '/', 1), '?', 1) AS domain from links where is_starting_link = 1 AND CrawlID IN (SELECT ID FROM Crawls_In_progress) GROUP BY CrawlID, domain), Links_To_Process AS (SELECT ID, Link, CrawlID, C1_TryCount, SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(Link, '/', 3), '://', -1), '/', 1), '?', 1) IN (SELECT domain FROM Starting_Domains WHERE CrawlID=l.CrawlID) AS IS_DOMAIN_IMPORTANT FROM Links l WHERE CrawlID IN (SELECT ID FROM Crawls_In_progress) AND (C1_TryCount < 3 OR C1_TryCount IS NULL) AND (TIMESTAMPDIFF(SECOND, C1_LastTry, NOW()) > 600 OR C1_LastTry IS NULL) AND C1_Success IS NULL) SELECT ID, Link, CrawlID, C1_TryCount FROM Links_To_Process ORDER BY IS_DOMAIN_IMPORTANT DESC LIMIT 20");
      if(!err) {
        var resultArray = Object.values(JSON.parse(JSON.stringify(rows)));
        Links = resultArray;
      }
      else {
        console.log(err);
      }
    }
    catch(ex)
    {
      console.log(ex);
    }
    finally
    {
        conn.release();
    }
  }
  catch(ex)
  {
    console.log(ex);
  }
  finally
  {
      return Links;
  }
}

async function UpdateLink(Link, ID) {
  try {
    const conn = await pool.getConnection();

    try {
      const [rows, fields, err] = await conn.query("UPDATE Links SET ? WHERE ID = ?", [Link, ID]);

      if(err) {
        console.log(err);
      }
    }
    catch(ex) {
      console.log(ex);
    }
    finally {
        conn.release();
    }
  }
  catch(ex) {
    console.log(ex);
  }
  finally {
  }
}

async function Cotroller1_Processor(Link) {
  try {
    var ID = Link.ID;
    delete Link.ID;
    Link = Object.assign(Link, {C1_Success: false, ResponseCode: null, ContentType: null, C1_SuccessDate: null, C1_Exception: null, C1_TryCount: Link.C1_TryCount + 1, C1_LastTry: null});

    try {
      const instance = axios.create();
      instance.defaults.timeout = 10000;
      const response = await instance.get(Link.Link);
      Link.ContentType = response.headers["content-type"];
      Link.ResponseCode = response.status;
    }
    catch(exc) {
      console.log(exc);
            try {
                  try {
                    Link.ResponseCode = exc.response.status;
                  } catch (e) {
                    console.log(e);
                    try {
                      Link.ResponseCode = exc.errno;
                    } catch (e) {
                    } finally {
                    }
                  } finally {
                  }
                }
                catch(ex)  {
                  console.log(ex);
                }
    }

    Link.C1_SuccessDate = new Date().toISOString().slice(0, 19).replace('T', ' ');
    Link.C1_Success = true;
  }
  catch(ex) {
    Link.C1_Exception = ex.stack;
    Link.C1_LastTry = new Date().toISOString().slice(0, 19).replace('T', ' ');
  }
  finally {
    if(Link.ResponseCode == "-3008")
    {
      if(await isOnline()) {
        await UpdateLink(Link, ID);
      }
      else {
        return "offline";
      }
    }
    else
    {
      await UpdateLink(Link, ID);
    }
  }

  await sleep(2000);
}

async function IsCrawlInProgress(CrawlID) {
  let val = true;

  try
  {
    const conn = await pool.getConnection();

    try
    {
      const [rows, fields] = await conn.query("SELECT Status AS Test FROM Crawls WHERE ID=?", [CrawlID]);
      val = rows[0].Test == "In Progress" ? true : false;
    }
    catch(ex)
    {
      val = false;
      console.log(ex);
    }
    finally
    {
        conn.release();
    }
  }
  catch(ex)
  {
    val = false;
    console.log(ex);
  }
  finally
  {
      return val;
  }
}

async function Controller1() {
  try {
    if (await Get_Controller1_Running() || await Get_Controller_Blocked()) {
      return;
    }

    await Set_Controller1_Running(true);

    var NoLinksLeft = false;
    var InternetWorking = await isOnline();
    while(!NoLinksLeft && !await Get_Controller_Blocked() && InternetWorking) {
      var Links = await GetLinksToController1();

      if(Links.length == 0) {
        NoLinksLeft = true;
      }
      else {
        for await (const link of Links) {
          if(await Get_Controller_Blocked()) {
            break;
          }

          var IsCrawlInProgressVar = await IsCrawlInProgress(link.CrawlID);

          if(!IsCrawlInProgressVar) {
            continue;
          }

          if(!await Get_Controller_Blocked()) {

            var s = await Cotroller1_Processor(link);

            if(s === "offline") {
              InternetWorking = false;
              break;
            }
          }
          else {
            break;
          }
        }
      }
    }

    if(!InternetWorking) {
      await sleep(60000);
    }

    await Set_Controller1_Running(false);
  }
  catch(ex) {
    console.log(ex);
    await Set_Controller1_Running(false);
  }
  finally {
  }
}












app.post('/GetLinksLeftToCrawl', async function (req, res) {
   res.header("Access-Control-Allow-Origin", "*");
   res.header("Access-Control-Allow-Headers", "Origin", "X-Requested-With", "Content-Type", "Accept");

   let AuthenticationToken = req.body.AuthenticationToken;
   let Page = req.body.Page;
   let CrawlID = req.body.CrawlID;

   if(await IsTokenValid(AuthenticationToken)) {
     var Crawlings = await GetLinksLeftToCrawlList(Page, CrawlID);

     if(Crawlings.Response) {
        res.json({Response: "Success", Crawlings: Crawlings.Crawlings, PageCount: Crawlings.PageCount, TotalCrawlings: Crawlings.TotalCrawlings});
     }
     else {
       res.json({Response: "Failure"});
     }
   }
   else {
     res.json({Response: "Failure"});
   }
})


async function GetLinksLeftToCrawlList(Page, CrawlID) {
  let Response = { Response: true, Crawlings: [], PageCount: await getPageCountToCrawl(CrawlID), TotalCrawlings: await getTotalCrawlingsCountToCrawl(CrawlID) };
  try
  {
    const conn = await pool.getConnection();

    try
    {
        var Offset2 = Page * 10 - 10;

        const [rows, fields, err] = await conn.query("SELECT Link, ResponseCode, ContentType FROM links WHERE CrawlID=? AND VisitSuccess IS NULL AND (VisitTryCount < 3 OR VisitTryCount IS NULL) LIMIT 10 OFFSET ?", [CrawlID, Offset2]);
        console.log(Offset2);
        console.log(CrawlID);

        if(!err) {
          var resultArray = Object.values(JSON.parse(JSON.stringify(rows)));
          console.log(resultArray);
          Response.Crawlings = resultArray;
        }
        else {
          console.log(err);
          Response.Response = false;
        }
    }
    catch(ex)
    {
      console.log(ex);
      Response.Response = false;
    }
    finally
    {
        conn.release();
    }
  }
  catch(ex)
  {
    console.log(ex);
    Response.Response = false;
  }
  finally
  {
      return Response;
  }
}





async function getPageCountToCrawl(CrawlID) {
  let val = 1;

  try
  {
    const conn = await pool.getConnection();

    try
    {
      const [rows, fields] = await conn.query("SELECT CEILING(COUNT(*)/10) AS Test FROM Links WHERE CrawlID=? AND VisitSuccess IS NULL AND (VisitTryCount < 3 OR VisitTryCount IS NULL);", [CrawlID]);
      val = rows[0].Test;
    }
    catch(ex)
    {
    }
    finally
    {
        conn.release();
    }
  }
  catch(ex)
  {
  }
  finally
  {
      return val;
  }
}

async function getTotalCrawlingsCountToCrawl(CrawlID) {
  let val = 0;

  try
  {
    const conn = await pool.getConnection();

    try
    {
      const [rows, fields] = await conn.query("SELECT COUNT(*) AS Test FROM Links WHERE CrawlID=? AND VisitSuccess IS NULL AND (VisitTryCount < 3 OR VisitTryCount IS NULL);", [CrawlID]);
      val = rows[0].Test;
    }
    catch(ex)
    {
    }
    finally
    {
        conn.release();
    }
  }
  catch(ex)
  {
  }
  finally
  {
      return val;
  }
}







app.post('/GetResponseCodeGrouped', async function (req, res) {
   res.header("Access-Control-Allow-Origin", "*");
   res.header("Access-Control-Allow-Headers", "Origin", "X-Requested-With", "Content-Type", "Accept");

   let AuthenticationToken = req.body.AuthenticationToken;
   let CrawlID = req.body.CrawlID;

   if(await IsTokenValid(AuthenticationToken)) {
         try
         {
           const conn = await pool.getConnection();
           var ResponseCodes = [];
           var Response = false;

           try
           {
             const [rows, fields, err] = await conn.query("SELECT ResponseCode, COUNT(*) AS Count FROM links WHERE ResponseCode IS NOT NULL AND IF(? IS NOT NULL, CrawlID=?, CrawlID<>0) GROUP BY ResponseCode ORDER BY COUNT DESC", [CrawlID, CrawlID]);

             if(!err) {
               ResponseCodes = Object.values(JSON.parse(JSON.stringify(rows)));
               Response = true;
             }

           }
           catch(ex)
           {

           }
           finally
           {
               conn.release();
           }
         }
         catch(ex)
         {

         }
         finally
         {
             res.json({Response: Response ? "Success" : "Failure", ...(Response && {ResponseCodes: ResponseCodes})});
         }
       }
       else {
         res.json({Response: "Failure"});
       }
})








app.post('/GetContentTypeGrouped', async function (req, res) {
   res.header("Access-Control-Allow-Origin", "*");
   res.header("Access-Control-Allow-Headers", "Origin", "X-Requested-With", "Content-Type", "Accept");

   let AuthenticationToken = req.body.AuthenticationToken;
   let CrawlID = req.body.CrawlID;

   if(await IsTokenValid(AuthenticationToken)) {
         try
         {
           const conn = await pool.getConnection();
           var ContentTypes = [];
           var Response = false;

           try
           {

             const [rows, fields, err] = await conn.query("SELECT ContentType, COUNT(*) AS Count FROM links WHERE ContentType IS NOT NULL AND IF(? IS NOT NULL, CrawlID=?, CrawlID<>0) GROUP BY ContentType ORDER BY COUNT DESC", [CrawlID, CrawlID]);

             if(!err) {
               ContentTypes = Object.values(JSON.parse(JSON.stringify(rows)));
               Response = true;
             }
           }
           catch(ex)
           {

           }
           finally
           {
               conn.release();
           }
         }
         catch(ex)
         {

         }
         finally
         {
             res.json({Response: Response ? "Success" : "Failure", ...(Response && {ContentTypes: ContentTypes})});
         }
       }
       else {
         res.json({Response: "Failure"});
       }
})























app.post('/GetLinksOfLink', async function (req, res) {
   res.header("Access-Control-Allow-Origin", "*");
   res.header("Access-Control-Allow-Headers", "Origin", "X-Requested-With", "Content-Type", "Accept");

   let AuthenticationToken = req.body.AuthenticationToken;
   let LinkID = req.body.LinkID;

   if(await IsTokenValid(AuthenticationToken)) {
         try
         {
           const conn = await pool.getConnection();
           var Links = [];
           var Response = false;

           try
           {

             const [rows, fields, err] = await conn.query("(SELECT TextHref, (SELECT Link FROM Links WHERE ID=lr.LinkID) AS Link FROM linkrelationships lr where parentid=?)", [LinkID]);

             if(!err) {
               Links = Object.values(JSON.parse(JSON.stringify(rows)));
               Response = true;
             }
           }
           catch(ex)
           {

           }
           finally
           {
               conn.release();
           }
         }
         catch(ex)
         {

         }
         finally
         {
             res.json({Response: Response ? "Success" : "Failure", ...(Response && {Links: Links})});
         }
       }
       else {
         res.json({Response: "Failure"});
       }
})





app.post('/GetPathways', async function (req, res) {
   res.header("Access-Control-Allow-Origin", "*");
   res.header("Access-Control-Allow-Headers", "Origin", "X-Requested-With", "Content-Type", "Accept");

   let AuthenticationToken = req.body.AuthenticationToken;
   let LinkID = req.body.LinkID;

   if(await IsTokenValid(AuthenticationToken)) {
         try
         {
           const conn = await pool.getConnection();
           var Links = [];
           var Response = false;

           try
           {
             const [rows, fields, err] = await conn.query("SELECT (SELECT Link FROM Links WHERE ID = ParentID) AS Source, TextHref FROM linkrelationships where linkid=?", [LinkID]);

             if(!err) {
               Links = Object.values(JSON.parse(JSON.stringify(rows)));
               Response = true;
             }
           }
           catch(ex)
           {

           }
           finally
           {
               conn.release();
           }
         }
         catch(ex)
         {

         }
         finally
         {
             res.json({Response: Response ? "Success" : "Failure", ...(Response && {Links: Links})});
         }
       }
       else {
         res.json({Response: "Failure"});
       }
})











app.post('/GetCrawlProgressDetails', async function (req, res) {
   res.header("Access-Control-Allow-Origin", "*");
   res.header("Access-Control-Allow-Headers", "Origin", "X-Requested-With", "Content-Type", "Accept");

   let AuthenticationToken = req.body.AuthenticationToken;
   let CrawlID = req.body.CrawlID;

   if(await IsTokenValid(AuthenticationToken)) {
         try
         {
           const conn = await pool.getConnection();
           var Details = [];
           var Response = false;
           var GranularDetail = [];

           try
           {
             const [rows, fields, err] = await conn.query("SELECT" +
               "(SELECT COUNT(*) FROM Links WHERE CrawlID=? AND C1_Success = 1 OR C1_Success IS NULL AND C1_TryCount >=3 AND CrawlID=?) AS Stage1_Complete," +
               "(SELECT COUNT(*) FROM Links WHERE CrawlID = ? AND VisitSuccess = 1 OR VisitSuccess IS NULL AND VisitTryCount >=3 AND CrawlID=?) AS Stage2_Complete," +
               "(SELECT COUNT(*) FROM Links WHERE CrawlID = ?) AS Total_Links," +
               "(SELECT Total_Links - Stage1_Complete) AS Stage1_Remaining," +
               "(SELECT Total_Links - Stage2_Complete) AS Stage2_Remaining", [CrawlID, CrawlID, CrawlID, CrawlID, CrawlID]);

             if(!err) {
               Details = Object.values(JSON.parse(JSON.stringify(rows)));

               const [rows2, fields2, err2] = await conn.query(
                 "WITH Starting_Domains AS (select DISTINCT(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(Link, '/', 3), '://', -1), '/', 1), ?, 1)) AS domain from links where is_starting_link = 1 AND CrawlID = ?),"+
                 "STAGE2_Visitable_Links AS (SELECT ID, Link, CrawlID, VisitTryCount, SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(Link, '/', 3), '://', -1), '/', 1), ?, 1) AS domain, (SELECT domain IN (SELECT domain FROM Starting_Domains WHERE CrawlID = ?) ) AS IS_DOMAIN_SAME FROM Links l WHERE CrawlID=? AND (VisitTryCount < 3 OR VisitTryCount IS NULL) AND (TIMESTAMPDIFF(SECOND, LastVisitTry, NOW()) > 600 OR LastVisitTry IS NULL) AND VisitSuccess IS NULL),"+
                 "STAGE1_Visitable_Links AS (SELECT ID, Link, CrawlID, VisitTryCount, SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(Link, '/', 3), '://', -1), '/', 1), ?, 1) AS domain, (SELECT domain IN (SELECT domain FROM Starting_Domains WHERE CrawlID = ?) ) AS IS_DOMAIN_SAME FROM Links l WHERE CrawlID=? AND (C1_TryCount < 3 OR C1_TryCount IS NULL) AND (TIMESTAMPDIFF(SECOND, C1_LastTry, NOW()) > 600 OR C1_LastTry IS NULL) AND C1_Success IS NULL),"+
                 "STAGE1_Completed_Links AS (SELECT ID, Link, CrawlID, VisitTryCount, SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(Link, '/', 3), '://', -1), '/', 1), ?, 1) AS domain, (SELECT domain IN (SELECT domain FROM Starting_Domains WHERE CrawlID = ?) ) AS IS_DOMAIN_SAME FROM Links l WHERE CrawlID=? AND C1_Success = 1 OR C1_Success IS NULL AND C1_TryCount >=3 AND CrawlID=?),"+

                 "STAGE2_Completed_Links AS (SELECT ID, Link, CrawlID, VisitTryCount, SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(Link, '/', 3), '://', -1), '/', 1), ?, 1) AS domain, (SELECT domain IN (SELECT domain FROM Starting_Domains WHERE CrawlID = ?) ) AS IS_DOMAIN_SAME FROM Links l WHERE CrawlID=? AND VisitSuccess = 1 OR VisitSuccess IS NULL AND VisitTryCount >=3 AND CrawlID=?)"+
                 "SELECT" +
                 "(SELECT COUNT(*) FROM STAGE2_Visitable_Links s2vl WHERE IS_DOMAIN_SAME = 1 OR s2vl.domain IN (SELECT domain FROM secondary_important_domains WHERE domain=s2vl.domain AND CrawlID=?)) AS STAGE2_LINKS_LEFT_IMPORTANT_DOMAIN, (SELECT COUNT(*) FROM STAGE2_Visitable_Links s2vl WHERE IS_DOMAIN_SAME = 0 AND s2vl.domain NOT IN (SELECT domain FROM secondary_important_domains WHERE domain=s2vl.domain AND CrawlID=?)) AS STAGE2_LINKS_LEFT_UNIMPORTANT_DOMAIN,"+
                 "(SELECT COUNT(*) FROM STAGE1_Visitable_Links s1vl WHERE IS_DOMAIN_SAME = 1 OR s1vl.domain IN (SELECT domain FROM secondary_important_domains WHERE domain=s1vl.domain AND CrawlID=?) ) AS STAGE1_LINKS_LEFT_IMPORTANT_DOMAIN, (SELECT COUNT(*) FROM STAGE1_Visitable_Links s1vl WHERE IS_DOMAIN_SAME = 0 AND s1vl.domain NOT IN (SELECT domain FROM secondary_important_domains WHERE domain=s1vl.domain AND CrawlID=?)) AS STAGE1_LINKS_LEFT_UNIMPORTANT_DOMAIN,"+
                 "(SELECT COUNT(*) FROM STAGE1_Completed_Links s1cl WHERE IS_DOMAIN_SAME = 1 OR s1cl.domain IN (SELECT domain FROM secondary_important_domains WHERE domain=s1cl.domain AND CrawlID=?)) AS STAGE1_LINKS_COMPLETED_IMPORTANT_DOMAIN, (SELECT COUNT(*) FROM STAGE1_Completed_Links s1cl WHERE IS_DOMAIN_SAME = 0 AND s1cl.domain NOT IN (SELECT domain FROM secondary_important_domains WHERE domain=s1cl.domain AND CrawlID=?)  ) AS STAGE1_LINKS_COMPLETED_UNIMPORTANT_DOMAIN,"+
                 "(SELECT COUNT(*) FROM STAGE2_Completed_Links s2cl WHERE IS_DOMAIN_SAME = 1 OR s2cl.domain IN (SELECT domain FROM secondary_important_domains WHERE domain=s2cl.domain AND CrawlID=?)) AS STAGE2_LINKS_COMPLETED_IMPORTANT_DOMAIN, (SELECT COUNT(*) FROM STAGE2_Completed_Links s2cl WHERE IS_DOMAIN_SAME = 0 AND s2cl.domain NOT IN (SELECT domain FROM secondary_important_domains WHERE domain=s2cl.domain AND CrawlID=?) ) AS STAGE2_LINKS_COMPLETED_UNIMPORTANT_DOMAIN"
                 , ['?', CrawlID, '?', CrawlID, CrawlID, '?', CrawlID, CrawlID, '?', CrawlID, CrawlID, CrawlID, '?', CrawlID, CrawlID, CrawlID, CrawlID, CrawlID, CrawlID, CrawlID, CrawlID, CrawlID, CrawlID, CrawlID]);

                if(!err2) {
                  Response = true;
                  GranularDetail = Object.values(JSON.parse(JSON.stringify(rows2)));
                }
             }

           }
           catch(ex)
           {
             console.log(ex);
           }
           finally
           {
               conn.release();
           }
         }
         catch(ex)
         {
           console.log(ex);
         }
         finally
         {
             res.json({Response: Response ? "Success" : "Failure", ...(Response && {Details: Details, GranularDetail: GranularDetail}  )});
         }
       }
       else {
         res.json({Response: "Failure"});
       }
})















app.post('/GetDomains', async function (req, res) {
   res.header("Access-Control-Allow-Origin", "*");
   res.header("Access-Control-Allow-Headers", "Origin", "X-Requested-With", "Content-Type", "Accept");

   let AuthenticationToken = req.body.AuthenticationToken;
   let CrawlID = req.body.CrawlID;

   if(await IsTokenValid(AuthenticationToken)) {
         try
         {
           const conn = await pool.getConnection();
           var Domains = [];
           var Response = false;

           try
           {
             const [rows, fields, err] = await conn.query("call saturn.GetDomains(?);", [CrawlID]);

             if(!err) {
               Domains = Object.values(JSON.parse(JSON.stringify(rows)));
               Response = true;
             }
           }
           catch(ex)
           {
             console.log(ex);
           }
           finally
           {
               conn.release();
           }
         }
         catch(ex)
         {
           console.log(ex);
         }
         finally
         {
             res.json({Response: Response ? "Success" : "Failure", ...(Response && {Domains: Domains})});
         }
       }
       else {
         res.json({Response: "Failure"});
       }
})






app.post('/GetStatus', async function (req, res) {
   res.header("Access-Control-Allow-Origin", "*");
   res.header("Access-Control-Allow-Headers", "Origin", "X-Requested-With", "Content-Type", "Accept");

   let AuthenticationToken = req.body.AuthenticationToken;
   let CrawlID = req.body.CrawlID;

   if(await IsTokenValid(AuthenticationToken)) {
         try
         {
           const conn = await pool.getConnection();
           var Status = "";
           var Response = false;

           try
           {
             const [rows, fields, err] = await conn.query("SELECT Status AS Test FROM Crawls WHERE ID=?", [CrawlID]);

             if(!err) {
               Status = rows[0].Test;
               Response = true;
             }
           }
           catch(ex)
           {
             console.log(ex);
           }
           finally
           {
               conn.release();
           }
         }
         catch(ex)
         {
           console.log(ex);
         }
         finally
         {
             res.json({Response: Response ? "Success" : "Failure", ...(Response && {Status: Status})});
         }
       }
       else {
         res.json({Response: "Failure"});
       }
})
















app.post('/DeleteCrawl', async function (req, res) {
   res.header("Access-Control-Allow-Origin", "*");
   res.header("Access-Control-Allow-Headers", "Origin", "X-Requested-With", "Content-Type", "Accept");

   let AuthenticationToken = req.body.AuthenticationToken;
   let CrawlID = req.body.CrawlID;

   if(await IsTokenValid(AuthenticationToken)) {
         try
         {
           const conn = await pool.getConnection();
           var Response = false;

           try
           {
             const [rows, fields, err] = await conn.query("call saturn.DeleteCrawl(?);", [CrawlID]);

             if(!err) {
               Response = true;
             }
           }
           catch(ex)
           {
             console.log(ex);
           }
           finally
           {
               conn.release();
           }
         }
         catch(ex)
         {
           console.log(ex);
         }
         finally
         {
             res.json({Response: Response ? "Success" : "Failure"});
         }
       }
       else {
         res.json({Response: "Failure"});
       }
})












app.post('/StartCrawl', async function (req, res) {
   res.header("Access-Control-Allow-Origin", "*");
   res.header("Access-Control-Allow-Headers", "Origin", "X-Requested-With", "Content-Type", "Accept");

   let AuthenticationToken = req.body.AuthenticationToken;
   let CrawlID = req.body.CrawlID;

   if(await IsTokenValid(AuthenticationToken)) {
         try
         {
           const conn = await pool.getConnection();
           var Response = false;

           try
           {
             const [rows, fields, err] = await conn.query("UPDATE Crawls SET Status='In Progress' WHERE ID = ? ", [CrawlID]);

             if(!err) {
               Response = true;
             }
           }
           catch(ex)
           {
             console.log(ex);
           }
           finally
           {
               conn.release();
           }
         }
         catch(ex)
         {
           console.log(ex);
         }
         finally
         {
             res.json({Response: Response ? "Success" : "Failure"});
         }
       }
       else {
         res.json({Response: "Failure"});
       }
})




app.post('/RestartCrawl', async function (req, res) {
   res.header("Access-Control-Allow-Origin", "*");
   res.header("Access-Control-Allow-Headers", "Origin", "X-Requested-With", "Content-Type", "Accept");

   let AuthenticationToken = req.body.AuthenticationToken;
   let CrawlID = req.body.CrawlID;

   if(await IsTokenValid(AuthenticationToken)) {
         try
         {
           await Set_Controller_Blocked(true);

           while(await Get_Controller1_Running() || await Get_Controller2_Running()) {
              await new Promise(resolve => setTimeout(resolve, 5000));
              await Set_Controller_Blocked(true);
           }

           console.log("looks like c1 and c2 and not running, we will sleep 6000 sec and then restartcrawl");

           await new Promise(resolve => setTimeout(resolve, 6000));

           const conn = await pool.getConnection();
           var Response = false;

           try
           {
             const [rows, fields, err] = await conn.query("CALL saturn.RestartCrawl(?);", [CrawlID]);

             if(!err) {
                 Response = true;
             }
             else {
               console.log(err);
             }
           }
           catch(ex)
           {
             console.log(ex);
           }
           finally
           {
               conn.release();
           }
         }
         catch(ex)
         {
           console.log(ex);
         }
         finally
         {
             await Set_Controller_Blocked(false);
             res.json({Response: Response ? "Success" : "Failure"});
         }
       }
       else {
         await Set_Controller_Blocked(false);
         res.json({Response: "Failure"});
       }
})



app.post('/StopCrawl', async function (req, res) {
   res.header("Access-Control-Allow-Origin", "*");
   res.header("Access-Control-Allow-Headers", "Origin", "X-Requested-With", "Content-Type", "Accept");

   let AuthenticationToken = req.body.AuthenticationToken;
   let CrawlID = req.body.CrawlID;

   if(await IsTokenValid(AuthenticationToken)) {
         try
         {
           const conn = await pool.getConnection();
           var Response = false;

           try
           {
             const [rows, fields, err] = await conn.query("UPDATE Crawls SET Status='Stopped' WHERE ID = ? ", [CrawlID]);

             if(!err) {
               Response = true;
             }
           }
           catch(ex)
           {
             console.log(ex);
           }
           finally
           {
               conn.release();
           }
         }
         catch(ex)
         {
           console.log(ex);
         }
         finally
         {
             res.json({Response: Response ? "Success" : "Failure"});
         }
       }
       else {
         res.json({Response: "Failure"});
       }
})















app.post('/SetDomainVisitable', async function (req, res) {
   res.header("Access-Control-Allow-Origin", "*");
   res.header("Access-Control-Allow-Headers", "Origin", "X-Requested-With", "Content-Type", "Accept");

   let AuthenticationToken = req.body.AuthenticationToken;
   let CrawlID = req.body.CrawlID;
   let Domain = req.body.Domain;
   let Visibility = req.body.Visibility;

   if(await IsTokenValid(AuthenticationToken)) {
         try
         {
           const conn = await pool.getConnection();
           var Response = false;

           try
           {
             const [rows, fields, err] = await conn.query("call saturn.SetDomainVisitable(?, ?, ?);", [CrawlID, Domain, Visibility]);

             if(!err) {
               Response = true;
             }
             else {
               Response = false;
             }
           }
           catch(ex)
           {
             console.log(ex);
           }
           finally
           {
               conn.release();
           }
         }
         catch(ex)
         {
           console.log(ex);
         }
         finally
         {
             res.json({Response: Response ? "Success" : "Failure", ...(Response)});
         }
       }
       else {
         res.json({Response: "Failure"});
       }
})




async function Set_Controller2_Running(NewValue) {
  try {
    const conn = await pool.getConnection();

    try {
      const [rows, fields, err] = await conn.query("UPDATE Controllers SET C2_Running = ? where blocked <> 99", [NewValue] );

      if(err) {
        console.log(err);
      }
    }
    catch(ex) {
      console.log(ex);
    }
    finally {
        conn.release();
    }
  }
  catch(ex) {
    console.log(ex);
  }
  finally {
  }
}

async function Set_Controller1_Running(NewValue) {
  try {
    const conn = await pool.getConnection();

    try {
      const [rows, fields, err] = await conn.query("UPDATE Controllers SET C1_Running = ? where blocked <> 99", [NewValue] );

      if(err) {
        console.log(err);
      }
    }
    catch(ex) {
      console.log(ex);
    }
    finally {
        conn.release();
    }
  }
  catch(ex) {
    console.log(ex);
  }
  finally {
  }
}

async function Set_Controller_Blocked(NewValue) {
  try {
    const conn = await pool.getConnection();

    try {
      const [rows, fields, err] = await conn.query("UPDATE Controllers SET Blocked = ? where blocked <> 99", [NewValue] );

      if(err) {
        console.log(err);
      }
    }
    catch(ex) {
      console.log(ex);
    }
    finally {
        conn.release();
    }
  }
  catch(ex) {
    console.log(ex);
  }
  finally {
  }
}


async function Get_Controller_Blocked() {
  let val = true;

  try
  {
    const conn = await pool.getConnection();

    try
    {
      const [rows, fields] = await conn.query("SELECT Blocked AS Test FROM Controllers");
      val = rows[0].Test;
    }
    catch(ex)
    {
      console.log(ex);
    }
    finally
    {
        conn.release();
    }
  }
  catch(ex)
  {
    console.log(ex);
  }
  finally
  {
      return val;
  }
}

async function Get_Controller1_Running() {
  let val = true;

  try
  {
    const conn = await pool.getConnection();

    try
    {
      const [rows, fields] = await conn.query("SELECT C1_Running AS Test FROM Controllers");
      val = rows[0].Test;
    }
    catch(ex)
    {
      console.log(ex);
    }
    finally
    {
        conn.release();
    }
  }
  catch(ex)
  {
    console.log(ex);
  }
  finally
  {
      return val;
  }
}

async function Get_Controller2_Running() {
  let val = true;

  try
  {
    const conn = await pool.getConnection();

    try
    {
      const [rows, fields] = await conn.query("SELECT C2_Running AS Test FROM Controllers");
      val = rows[0].Test;
    }
    catch(ex)
    {
      console.log(ex);
    }
    finally
    {
        conn.release();
    }
  }
  catch(ex)
  {
    console.log(ex);
  }
  finally
  {
      return val;
  }
}




async function IsSeleniumGridOnline() {
  var returnable = false;

  try {
    const responseQueue = await axios.get("http://localhost:4444/se/grid/newsessionqueue/queue");
    var count = responseQueue.data.value.length;

    if(count >= 0) {
      returnable = true;
    }
  }
  catch(ex) {
    console.log(ex);
  }

  return returnable;
}





async function GetMaximumSessions() {
  var returnable = 0;

  try {
    const responseQueue = await axios.get("http://localhost:4444/status");
    returnable = responseQueue.data.value.nodes[0].maxSessions;
  }
  catch(ex) {
    console.log(ex);
  }

  return returnable;
}



async function SetSeleniumGridLocalhostStatus(Status) {
  try {
    const conn = await pool.getConnection();

    try {
      const [rows, fields, err] = await conn.query("UPDATE Agents SET Status = ? WHERE Name = ?", [Status, "localhost"]);
    }
    catch(ex) {
    } finally {
        conn.release();
    }
  }
  catch(ex) {
  }
  finally {
  }
}


app.post('/SetLocalhostSeleniumOff', async function (req, res) {
   res.header("Access-Control-Allow-Origin", "*");
   res.header("Access-Control-Allow-Headers", "Origin", "X-Requested-With", "Content-Type", "Accept");

   let AuthenticationToken = req.body.AuthenticationToken;

   if(await IsTokenValid(AuthenticationToken)) {
     var commandToExecute = "call " + path.join(__dirname, "closeCommandLines.bat");
     exec(commandToExecute);
     await sleep(7000);

     if(await IsSeleniumGridOnline()) {
       res.json({Response: "Failure"});
    }
    else {
      res.json({Response: "Success"});
      SetSeleniumGridLocalhostStatus(0);
    }

   }
   else {
     res.json({Response: "Failure"});
   }
})



app.post('/SetLocalhostSeleniumOn', async function (req, res) {
   res.header("Access-Control-Allow-Origin", "*");
   res.header("Access-Control-Allow-Headers", "Origin", "X-Requested-With", "Content-Type", "Accept");

   let AuthenticationToken = req.body.AuthenticationToken;

   if(await IsTokenValid(AuthenticationToken)) {
     var lhi = await GetLocalhostInformation();
     var commandToExecute = "call " + path.join(__dirname, "closeCommandLines.bat");
     exec(commandToExecute);
     await sleep(5000);
     var commandToExecute2 = "start cmd /c " + path.join(__dirname, "selenium_grid.bat " + lhi.MaximumSessions);
     exec(commandToExecute2);
     await sleep(15000);

     if(await IsSeleniumGridOnline()) {
       res.json({Response: "Success"});
       SetSeleniumGridLocalhostStatus(1);
    }
    else {
      res.json({Response: "Failure"});
    }
   }
   else {
     res.json({Response: "Failure"});
   }
})







app.post('/GetLocalSeleniumGridInformation', async function (req, res) {
   res.header("Access-Control-Allow-Origin", "*");
   res.header("Access-Control-Allow-Headers", "Origin", "X-Requested-With", "Content-Type", "Accept");

   let AuthenticationToken = req.body.AuthenticationToken;

   if(await IsTokenValid(AuthenticationToken)) {
     var Status = await IsSeleniumGridOnline();

     if(!Status) {
       res.json({Status: Status, Sessions: 0});
    }
    else {
      var Sessions = await GetMaximumSessions();
      res.json({Status: Status, Sessions: Sessions});
    }
   }
   else {
     res.json({Response: "Failure"});
   }
})



async function SetLocalhostMaximumSessions(MaximumSessions) {
  try {
    const conn = await pool.getConnection();

    try {
      const [rows, fields, err] = await conn.query("UPDATE Agents SET MaximumSessions = ? WHERE Name = ?", [MaximumSessions, "localhost"]);
    }
    catch(ex) {
    } finally {
        conn.release();
    }
  }
  catch(ex) {
  }
  finally {
  }
}

//// TODO:
app.post('/ChangeLocalhostSeleniumGridMaximumSessions', async function (req, res) {
   res.header("Access-Control-Allow-Origin", "*");
   res.header("Access-Control-Allow-Headers", "Origin", "X-Requested-With", "Content-Type", "Accept");

   let AuthenticationToken = req.body.AuthenticationToken;
   let MaximumSessions = req.body.MaximumSessions;

   console.log("max sessions is: " + MaximumSessions);
   await sleep(5000);

   if(await IsTokenValid(AuthenticationToken)) {
     await SetLocalhostMaximumSessions(MaximumSessions);

     var lhi = await GetLocalhostInformation();
     var commandToExecute = "call " + path.join(__dirname, "closeCommandLines.bat");
     exec(commandToExecute);
     await sleep(5000);
     var commandToExecute2 = "start cmd /c " + path.join(__dirname, "selenium_grid.bat " + lhi.MaximumSessions);
     exec(commandToExecute2);
     await sleep(15000);

     if(await IsSeleniumGridOnline()) {
       res.json({Response: "Success"});
     }
     else {
       res.json({Response: "Failure"});
     }

   }
   else {
     res.json({Response: "Failure"});
   }
})













var ReformatterRunning = false;

async function Reformatter() {
  try {
    if (ReformatterRunning) {
      return;
    }

    ReformatterRunning = true;

    console.log("reformatter running");






  }
  catch(ex) {
    console.log(ex);
  }
  finally {
    ReformatterRunning = false;
  }
}
















Setup().then(() => {
  app.listen(port, () => {
    console.log(new Date() + ":  Example app listening at http://localhost:" + port);
  });
});
