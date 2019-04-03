var AWS = require("aws-sdk");
var https = require("https");
var s3 = new AWS.S3({ apiVersion: "2006-03-01", region: "us-west-2" });
var customer_id = process.env.CUSTOMER_ID;
var campaign_id = process.env.CAMPAIGN_ID;
var api_key = process.env.API_KEY;

exports.handler = async (event) => {
    if (!event.body) {
        return formatResponse(400, { error: "Empty request body." });
    }

    let transcodedData;

    try {
        let data = JSON.parse(event.body);
        transcodedData = normalizeRequestData(data);
    } catch (e) {
        return formatResponse(400, { error: e.message });
    }

    await logToFile("transcoded.json", transcodedData);

    let response = await sendCensus(transcodedData);

    if (response.statusCode >= 400) {
        return formatResponse(response.statusCode, { error: response.body });
    }

    let normalizedResponse;

    try {
        let data = JSON.parse(response.body);
        await logToFile("result.json", data);
        normalizedResponse = normalizeResponseData(data);
    } catch (e) {
        return formatResponse(400, { error: e.message });
    }

    return formatResponse(200, normalizedResponse);
};

function normalizeRequestData(data) {
    if (!data) {
        throw new Error("Request is empty.");
    }

    let census = data.Census;
    if (!census) {
        throw new Error("Invalid request data.");
    }

    let rows = [];
    for (let i = 0, l = census.length; i < l; i++) {
        rows.push(normalizeItem(census[i]));
    }
    
    return {
        rows: rows
    };
}

function normalizeItem(item) {
    if (!item) {
        throw new Error("Invalid request.");
    }

    let name = parseName(item.Name);
    let dob = parseDOB(item.DOB);
    let address = parseAddress(item.Address);
    let phone = parsePhone(item.Phone);
    let sex = parseGender(item.Gender);

    return {
        pii: {
            first_name: name.first,
            last_name: name.last,
            dob: dob,
            address: address.street,
            city: address.city,
            state: address.state,
            zip: address.zip,
            phone: phone,
            sex: sex
        }
    };
}

function parseName(fullName) {
    if (!fullName) {
        throw new Error("Invalid request.");
    }

    let nameArr = fullName.split(" ");
    let length = nameArr.length;
    if (length === 0) {
        throw new Error("Invalid name.");
    }

    let first = nameArr[0];
    let last = "";
    if (length > 1) {
        last = nameArr[length - 1];
    }

    return {
        first: first,
        last: last
    };
}

function parseDOB(dob) {
    if (!dob) {
        throw new Error("Invalid date of birth.");
    }

    return dob.substr(0, 6) + dob.substr(8);
}

function parseAddress(address) {
    if (!address) {
        throw new Error("Invalid address.");
    }

    let addressArr = address.split(", ");
    if (addressArr.length !== 3) {
        throw new Error("Invalid address.");
    }

    let stateZip = addressArr[addressArr.length - 1].split(" ");
    if (stateZip.length !== 2) {
        throw new Error("Invalid address.");
    }

    return {
        street: addressArr[0],
        city: addressArr[1],
        state: stateZip[0],
        zip: stateZip[1]
    };
}

function parsePhone(phone) {
    if (!phone) {
        throw new Error("Invalid phone number.");
    }

    return phone;
}

function parseGender(gender) {
    if (!gender) {
        throw new Error("Invalid gender.");
    }

    return gender[0].toUpperCase();
}

function formatResponse(code, data) {
    return {
        statusCode: code,
        headers: {
            "Content-Type": "application/json; charset=utf-8"
        },
        body: JSON.stringify(data)
    };
}

function sendCensus(requestData) {
    let jsonObject = JSON.stringify(requestData);

    let options = {
        hostname: "api.alumai.com",
        port: 443,
        path: `/census/v1/customer/${customer_id}/campaign/${campaign_id}/inline/test`,
        method: "POST",
        headers: {
            "x-api-key": api_key,
            "Content-Type": "application/json",
            "Content-Length": Buffer.byteLength(jsonObject, "utf8")
        }
      };

    return new Promise(function(resolve, reject) {
        let req = https.request(options, function(res) {
            let response = {
                statusCode: res.statusCode,
                headers: res.headers,
                body: []
            };

            res.on("data", function(chunk) {
                response.body.push(chunk);
            });

            res.on("end", function() {
                if (response.body.length) {
                    response.body = response.body.join("");
                } else {
                    response.body = "";
                }

                resolve(response);
            });
        });

        req.on("error", function(error) {
            resolve(formatResponse(500, { error: error.message || "Internal server error." }));
        });

        req.write(jsonObject);
        req.end();
    });
}

function normalizeResponseData(data) {
    if (!data) {
        throw new Error("Response body is empty.");
    }

    let normalizedData = {};

    if (data.runTime) {
        normalizedData.runTime = data.runTime;
    }

    if (data.memberO2Scores) {
        normalizedData.memberO2Scores = data.memberO2Scores;
    }

    if (data.state) {
        normalizedData.state = data.state;
    }

    return normalizedData;
}

function logToFile(fileName, data) {
    return new Promise(function(resolve, reject) {
        let params = {
            Bucket: "verikai-api",
            Key: fileName,
            Body: JSON.stringify(data)
        };

        s3.putObject(params, function(err, data) {
            if (err) {
                console.log(err);
            }
            resolve();
        });
    });
}
